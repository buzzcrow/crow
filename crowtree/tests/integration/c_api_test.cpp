// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// PT8: exercise the stable C ABI surface (open/apply/get/scan/snapshot/reopen,
// snapshot view iteration, and export/import round-trip).
#include "crowtree/c_api.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <map>
#include <string>
#include <vector>

namespace
{
std::string make_key(int i)
{
    std::array<char, 16> b{};
    snprintf(b.data(), b.size(), "key%05d", i);
    return b.data();
}

ct_status put_flush(ct_tree *t, uint64_t slot, const std::string &k, const std::string &v)
{
    ct_status s = ct_apply_put(t, slot, reinterpret_cast<const uint8_t *>(k.data()), k.size(),
                               reinterpret_cast<const uint8_t *>(v.data()), v.size());
    if (s != 0) {
        return s;
    }
    return ct_flush(t);
}
} // namespace

TEST(CApi, MemOpenApplyGetScan)
{
    ct_options opt  = {};
    opt.frame_bytes = 4096;
    ct_tree *t      = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);
    ASSERT_NE(t, nullptr);

    for (int i = 0; i < 30; ++i) {
        ASSERT_EQ(put_flush(t, i + 1, make_key(i), "v" + std::to_string(i)), 0);
    }
    // Point read.
    int32_t  found = 0;
    uint64_t slot  = 0;
    ct_buf   val   = {};
    ASSERT_EQ(ct_get(t, reinterpret_cast<const uint8_t *>(make_key(5).data()), make_key(5).size(), &found, &slot, &val),
              0);
    ASSERT_EQ(found, 1);
    EXPECT_EQ(std::string(reinterpret_cast<char *>(val.data), val.len), "v5");
    ct_free_buf(&val);

    // Delete.
    std::string k7 = make_key(7);
    ASSERT_EQ(ct_apply_delete(t, 1000, reinterpret_cast<const uint8_t *>(k7.data()), k7.size()), 0);
    ct_force_advance_slot(t, 1000);
    ASSERT_EQ(ct_flush(t), 0);
    ASSERT_EQ(ct_get(t, reinterpret_cast<const uint8_t *>(k7.data()), k7.size(), &found, &slot, &val), 0);
    EXPECT_EQ(found == 1, false);
    ct_free_buf(&val);

    // scan packs records: [u32 klen][key][u64 slot][u32 vlen][val]*.
    ct_buf   entries = {};
    uint64_t count   = 0;
    int32_t  trunc   = 0;
    ASSERT_EQ(ct_scan(t, nullptr, 0, 0, &entries, &count, &trunc), 0);
    EXPECT_EQ(count, 29U); // 30 puts - 1 delete
    ct_free_buf(&entries);

    ct_close(t);
}

// plan-tree #20: ct_apply_batch lets a caller (crowkv's CrowtreeEngine) apply
// several ops atomically at one slot in a single call into Crowtree::apply,
// instead of looping ct_apply_put/ct_apply_delete per key (which would let a
// concurrent reader observe a partially applied batch).
TEST(CApi, ApplyBatchAtomicMultiKey)
{
    ct_options opt  = {};
    opt.frame_bytes = 4096;
    ct_tree *t      = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);

    // Pack [u8 kind][u32 klen][key][u32 vlen][value] * 3: put a, put b, delete c
    // (c doesn't exist yet -- exercises a delete-of-absent-key record too).
    auto pack_record = [](std::string *o, uint8_t kind, const std::string &k, const std::string &v) {
        o->push_back(static_cast<char>(kind));
        for (int i = 0; i < 4; ++i) {
            o->push_back(static_cast<char>((static_cast<uint32_t>(k.size()) >> (8 * i)) & 0xff));
        }
        o->append(k);
        for (int i = 0; i < 4; ++i) {
            o->push_back(static_cast<char>((static_cast<uint32_t>(v.size()) >> (8 * i)) & 0xff));
        }
        o->append(v);
    };
    std::string packed;
    pack_record(&packed, 0, "a", "va");
    pack_record(&packed, 0, "b", "vb");
    pack_record(&packed, 1, "c", "");
    ASSERT_EQ(ct_apply_batch(t, 1, reinterpret_cast<const uint8_t *>(packed.data()), packed.size(), 3), 0);
    ASSERT_EQ(ct_flush(t), 0);

    for (const auto &kv : std::vector<std::pair<std::string, std::string>>{
             {"a", "va"},
             {"b", "vb"}
    }) {
        int32_t  found = 0;
        uint64_t slot  = 0;
        ct_buf   val   = {};
        ASSERT_EQ(ct_get(t, reinterpret_cast<const uint8_t *>(kv.first.data()), kv.first.size(), &found, &slot, &val),
                  0);
        ASSERT_EQ(found, 1) << kv.first;
        EXPECT_EQ(std::string(reinterpret_cast<char *>(val.data), val.len), kv.second);
        EXPECT_EQ(slot, 1U);
        ct_free_buf(&val);
    }

    // Same-batch duplicate key: last occurrence wins (mirrors MemTable::apply_batch).
    std::string dup;
    pack_record(&dup, 0, "d", "first");
    pack_record(&dup, 0, "d", "second");
    ASSERT_EQ(ct_apply_batch(t, 2, reinterpret_cast<const uint8_t *>(dup.data()), dup.size(), 2), 0);
    ASSERT_EQ(ct_flush(t), 0);
    int32_t  found = 0;
    uint64_t slot  = 0;
    ct_buf   val   = {};
    ASSERT_EQ(ct_get(t, reinterpret_cast<const uint8_t *>("d"), 1, &found, &slot, &val), 0);
    ASSERT_EQ(found, 1);
    EXPECT_EQ(std::string(reinterpret_cast<char *>(val.data), val.len), "second");
    ct_free_buf(&val);

    // Malformed input (count says 2 records but only 1 fits) is rejected, not UB.
    std::string short_buf;
    pack_record(&short_buf, 0, "e", "ve");
    EXPECT_EQ(ct_apply_batch(t, 3, reinterpret_cast<const uint8_t *>(short_buf.data()), short_buf.size(), 2),
              static_cast<ct_status>(-2)); // kInvalidArgument

    ct_close(t);
}

// plan-tree #5 B2d: ct_apply_put/ct_apply_delete/ct_apply_batch now build a
// Crowtree::encoded_op vector (key+pre-encoded cell) and call apply_encoded
// instead of going through Batch/apply. apply_encoded re-implements the
// oversized-key guard (plan-tree #15) independently of apply()'s -- exercise
// all three call paths through the C boundary to confirm that guard still
// fires (and, for the batch case, still rejects atomically -- no partial
// writes) after the refactor.
TEST(CApi, OversizedKeyRejectedThroughEncodedPath)
{
    ct_options opt   = {};
    opt.frame_bytes  = 4096; // default limit = frame_bytes / 2 = 2048
    ct_tree     *t   = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);

    std::string big_key(3000, 'x');
    std::string ok_key = "ok";

    EXPECT_EQ(ct_apply_put(t, 1, reinterpret_cast<const uint8_t *>(big_key.data()), big_key.size(),
                          reinterpret_cast<const uint8_t *>("v"), 1),
             static_cast<ct_status>(-2)); // kInvalidArgument
    EXPECT_EQ(
        ct_apply_delete(t, 1, reinterpret_cast<const uint8_t *>(big_key.data()), big_key.size()),
        static_cast<ct_status>(-2));

    auto pack_record = [](std::string *o, uint8_t kind, const std::string &k, const std::string &v) {
        o->push_back(static_cast<char>(kind));
        for (int i = 0; i < 4; ++i) {
            o->push_back(static_cast<char>((static_cast<uint32_t>(k.size()) >> (8 * i)) & 0xff));
        }
        o->append(k);
        for (int i = 0; i < 4; ++i) {
            o->push_back(static_cast<char>((static_cast<uint32_t>(v.size()) >> (8 * i)) & 0xff));
        }
        o->append(v);
    };
    std::string packed;
    pack_record(&packed, 0, ok_key, "v1");    // fine on its own
    pack_record(&packed, 0, big_key, "v2");   // poisons the whole batch
    EXPECT_EQ(ct_apply_batch(t, 1, reinterpret_cast<const uint8_t *>(packed.data()), packed.size(), 2),
             static_cast<ct_status>(-2));

    // All-or-nothing: the batch's *other*, otherwise-valid key must not have
    // landed either.
    ASSERT_EQ(ct_flush(t), 0);
    int32_t  found = 0;
    uint64_t slot  = 0;
    ct_buf   val   = {};
    ASSERT_EQ(ct_get(t, reinterpret_cast<const uint8_t *>(ok_key.data()), ok_key.size(), &found, &slot, &val), 0);
    EXPECT_EQ(found, 0);
    ct_free_buf(&val);

    ct_close(t);
}

TEST(CApi, FileCheckpointReopen)
{
    std::array<char, 32> tmpl{"/tmp/crowtree_capi_XXXXXX"};
    char                *d = mkdtemp(tmpl.data());
    ASSERT_NE(d, nullptr);
    std::string path(d);

    ct_options opt  = {};
    opt.path        = path.c_str();
    opt.iu_size     = 1;
    opt.frame_bytes = 4096;
    opt.compression = 1; // lz4 (falls back to stored-raw if unavailable)
    // Default backend is CT_BACKEND_TEXT (TextPageStore debug backend)

    std::map<std::string, std::string> oracle;
    {
        ct_tree *t = nullptr;
        ASSERT_EQ(ct_open(&opt, &t), 0);
        for (int i = 0; i < 50; ++i) {
            std::string v = "value" + std::to_string(i);
            ASSERT_EQ(put_flush(t, i + 1, make_key(i), v), 0);
            oracle[make_key(i)] = v;
        }
        uint64_t durable = 0;
        ASSERT_EQ(ct_snapshot(t, &durable), 0);
        EXPECT_EQ(durable, 50U);
        ct_close(t);
    }
    {
        ct_tree *t = nullptr;
        ASSERT_EQ(ct_open(&opt, &t), 0);
        EXPECT_EQ(ct_last_applied_slot(t), 50U);
        for (const auto &kv : oracle) {
            int32_t  found = 0;
            uint64_t slot  = 0;
            ct_buf   val   = {};
            ASSERT_EQ(
                ct_get(t, reinterpret_cast<const uint8_t *>(kv.first.data()), kv.first.size(), &found, &slot, &val), 0);
            ASSERT_EQ(found, 1) << "missing " << kv.first;
            EXPECT_EQ(std::string(reinterpret_cast<char *>(val.data), val.len), kv.second);
            ct_free_buf(&val);
        }
        ct_close(t);
    }
}

// plan-tree #22: ct_options.backend=1 selects BlockPageStore (O_DIRECT)
// instead of the default FilePageStore -- same round-trip as
// FileCheckpointReopen above, just through the raw-block-device backend.
TEST(CApi, BlockDeviceCheckpointReopen)
{
    std::array<char, 32> tmpl{"/tmp/crowtree_capi_XXXXXX"};
    char                *d = mkdtemp(tmpl.data());
    ASSERT_NE(d, nullptr);
    std::string path(d);

    ct_options opt  = {};
    opt.path        = path.c_str();
    opt.iu_size     = 1;
    opt.frame_bytes = 4096;
    opt.backend     = CT_BACKEND_BLOCK; // BlockPageStore
    opt.block_size  = 8 * 1024;         // 8 KiB blocks for testing

    std::map<std::string, std::string> oracle;
    {
        ct_tree *t = nullptr;
        ASSERT_EQ(ct_open(&opt, &t), 0);
        for (int i = 0; i < 50; ++i) {
            std::string v = "value" + std::to_string(i);
            ASSERT_EQ(put_flush(t, i + 1, make_key(i), v), 0);
            oracle[make_key(i)] = v;
        }
        uint64_t durable = 0;
        ASSERT_EQ(ct_snapshot(t, &durable), 0);
        EXPECT_EQ(durable, 50U);
        ct_close(t);
    }
    {
        ct_tree *t = nullptr;
        ASSERT_EQ(ct_open(&opt, &t), 0);
        EXPECT_EQ(ct_last_applied_slot(t), 50U);
        for (const auto &kv : oracle) {
            int32_t  found = 0;
            uint64_t slot  = 0;
            ct_buf   val   = {};
            ASSERT_EQ(
                ct_get(t, reinterpret_cast<const uint8_t *>(kv.first.data()), kv.first.size(), &found, &slot, &val), 0);
            ASSERT_EQ(found, 1) << "missing " << kv.first;
            EXPECT_EQ(std::string(reinterpret_cast<char *>(val.data), val.len), kv.second);
            ct_free_buf(&val);
        }
        ct_close(t);
    }
}

TEST(CApi, SnapshotViewIterate)
{
    ct_options opt = {};
    ct_tree   *t   = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);
    for (int i = 0; i < 10; ++i) {
        ASSERT_EQ(put_flush(t, i + 1, make_key(i), "v" + std::to_string(i)), 0);
    }
    ct_view *v = nullptr;
    ASSERT_EQ(ct_snapshot_view(t, &v), 0);
    EXPECT_EQ(ct_view_at_slot(v), 10U);
    ct_iter *it = nullptr;
    ASSERT_EQ(ct_view_iter(v, &it), 0);
    int seen = 0;
    while (true) {
        ct_buf   key   = {};
        ct_buf   val   = {};
        uint64_t slot  = 0;
        uint8_t  kind  = 0;
        int32_t  valid = 0;
        ASSERT_EQ(ct_iter_next(it, &key, &slot, &kind, &val, &valid), 0);
        if (valid == 0) {
            ct_free_buf(&key);
            ct_free_buf(&val);
            break;
        }
        ++seen;
        ct_free_buf(&key);
        ct_free_buf(&val);
    }
    EXPECT_EQ(seen, 10);
    ct_iter_release(it);
    ct_view_release(v);
    ct_close(t);
}

TEST(CApi, SnapshotExportImport)
{
    ct_options opt = {};
    ct_tree   *a   = nullptr;
    ASSERT_EQ(ct_open(&opt, &a), 0);
    for (int i = 0; i < 40; ++i) {
        ASSERT_EQ(put_flush(a, i + 1, make_key(i), "v" + std::to_string(i)), 0);
    }
    ct_tree *b = nullptr;
    ASSERT_EQ(ct_open(&opt, &b), 0);

    ct_export *e = nullptr;
    ASSERT_EQ(ct_snapshot_export_begin(a, &e), 0);
    ct_import *im = nullptr;
    ASSERT_EQ(ct_snapshot_import_begin(b, &im), 0);
    while (true) {
        ct_buf  chunk = {};
        int32_t done  = 0;
        ASSERT_EQ(ct_snapshot_export_next(e, &chunk, &done), 0);
        if (chunk.len > 0) {
            ASSERT_EQ(ct_snapshot_import_feed(im, chunk.data, chunk.len), 0);
        }
        ct_free_buf(&chunk);
        if (done != 0) {
            break;
        }
    }
    ct_snapshot_export_end(e);
    uint64_t at = 0;
    ASSERT_EQ(ct_snapshot_import_finish(im, &at), 0);
    ct_snapshot_import_end(im);
    EXPECT_EQ(at, 40U);

    for (int i = 0; i < 40; ++i) {
        int32_t     found = 0;
        uint64_t    slot  = 0;
        ct_buf      val   = {};
        std::string k     = make_key(i);
        ASSERT_EQ(ct_get(b, reinterpret_cast<const uint8_t *>(k.data()), k.size(), &found, &slot, &val), 0);
        ASSERT_EQ(found, 1) << "missing " << k;
        EXPECT_EQ(std::string(reinterpret_cast<char *>(val.data), val.len), "v" + std::to_string(i));
        ct_free_buf(&val);
    }
    ct_close(a);
    ct_close(b);
}
