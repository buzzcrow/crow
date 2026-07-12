// PT8: exercise the stable C ABI surface (open/apply/get/scan/checkpoint/reopen,
// snapshot view iteration, and export/import round-trip).
#include "crowtree/c_api.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <map>
#include <string>
#include <vector>

namespace {
std::string Key(int i) {
  char b[16];
  snprintf(b, sizeof(b), "key%05d", i);
  return b;
}
ct_status PutFlush(ct_tree* t, uint64_t slot, const std::string& k, const std::string& v) {
  ct_status s = ct_apply_put(t, slot, reinterpret_cast<const uint8_t*>(k.data()), k.size(),
                             reinterpret_cast<const uint8_t*>(v.data()), v.size());
  if (s != 0) {
    return s;
  }
  return ct_flush(t);
}
}  // namespace

TEST(CApi, MemOpenApplyGetScan) {
  ct_options opt = {};
  opt.frame_bytes = 4096;
  ct_tree* t = nullptr;
  ASSERT_EQ(ct_open(&opt, &t), 0);
  ASSERT_NE(t, nullptr);

  for (int i = 0; i < 30; ++i) {
    ASSERT_EQ(PutFlush(t, i + 1, Key(i), "v" + std::to_string(i)), 0);
  }
  // Point read.
  int32_t found = 0;
  uint64_t slot = 0;
  ct_buf val = {};
  ASSERT_EQ(ct_get(t, reinterpret_cast<const uint8_t*>(Key(5).data()), Key(5).size(), &found, &slot,
                   &val),
            0);
  ASSERT_EQ(found, 1);
  EXPECT_EQ(std::string(reinterpret_cast<char*>(val.data), val.len), "v5");
  ct_free_buf(&val);

  // Delete.
  std::string k7 = Key(7);
  ASSERT_EQ(ct_apply_delete(t, 1000, reinterpret_cast<const uint8_t*>(k7.data()), k7.size()), 0);
  ct_force_advance_slot(t, 1000);
  ASSERT_EQ(ct_flush(t), 0);
  ASSERT_EQ(ct_get(t, reinterpret_cast<const uint8_t*>(k7.data()), k7.size(), &found, &slot, &val),
            0);
  EXPECT_EQ(found, 0);
  ct_free_buf(&val);

  // scan packs records: [u32 klen][key][u64 slot][u32 vlen][val]*.
  ct_buf entries = {};
  uint64_t count = 0;
  int32_t trunc = 0;
  ASSERT_EQ(ct_scan(t, nullptr, 0, 0, &entries, &count, &trunc), 0);
  EXPECT_EQ(count, 29u);  // 30 puts - 1 delete
  ct_free_buf(&entries);

  ct_close(t);
}

TEST(CApi, FileCheckpointReopen) {
  char tmpl[] = "/tmp/crowtree_capi_XXXXXX";
  int fd = mkstemp(tmpl);
  ASSERT_GE(fd, 0);
  close(fd);
  std::string path(tmpl);

  ct_options opt = {};
  opt.path = path.c_str();
  opt.iu_size = 4096;
  opt.frame_bytes = 4096;
  opt.compression = 1;  // lz4 (falls back to stored-raw if unavailable)

  std::map<std::string, std::string> oracle;
  {
    ct_tree* t = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);
    for (int i = 0; i < 50; ++i) {
      std::string v = "value" + std::to_string(i);
      ASSERT_EQ(PutFlush(t, i + 1, Key(i), v), 0);
      oracle[Key(i)] = v;
    }
    uint64_t durable = 0;
    ASSERT_EQ(ct_checkpoint(t, &durable), 0);
    EXPECT_EQ(durable, 50u);
    ct_close(t);
  }
  {
    ct_tree* t = nullptr;
    ASSERT_EQ(ct_open(&opt, &t), 0);
    EXPECT_EQ(ct_last_applied_slot(t), 50u);
    for (const auto& kv : oracle) {
      int32_t found = 0;
      uint64_t slot = 0;
      ct_buf val = {};
      ASSERT_EQ(ct_get(t, reinterpret_cast<const uint8_t*>(kv.first.data()), kv.first.size(),
                       &found, &slot, &val),
                0);
      ASSERT_EQ(found, 1) << "missing " << kv.first;
      EXPECT_EQ(std::string(reinterpret_cast<char*>(val.data), val.len), kv.second);
      ct_free_buf(&val);
    }
    ct_close(t);
  }
  std::remove(path.c_str());
}

TEST(CApi, SnapshotViewIterate) {
  ct_options opt = {};
  ct_tree* t = nullptr;
  ASSERT_EQ(ct_open(&opt, &t), 0);
  for (int i = 0; i < 10; ++i) {
    ASSERT_EQ(PutFlush(t, i + 1, Key(i), "v" + std::to_string(i)), 0);
  }
  ct_view* v = nullptr;
  ASSERT_EQ(ct_snapshot_view(t, &v), 0);
  EXPECT_EQ(ct_view_at_slot(v), 10u);
  ct_iter* it = nullptr;
  ASSERT_EQ(ct_view_iter(v, &it), 0);
  int seen = 0;
  while (true) {
    ct_buf key = {}, val = {};
    uint64_t slot = 0;
    uint8_t kind = 0;
    int32_t valid = 0;
    ASSERT_EQ(ct_iter_next(it, &key, &slot, &kind, &val, &valid), 0);
    if (!valid) {
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

TEST(CApi, SnapshotExportImport) {
  ct_options opt = {};
  ct_tree* a = nullptr;
  ASSERT_EQ(ct_open(&opt, &a), 0);
  for (int i = 0; i < 40; ++i) {
    ASSERT_EQ(PutFlush(a, i + 1, Key(i), "v" + std::to_string(i)), 0);
  }
  ct_tree* b = nullptr;
  ASSERT_EQ(ct_open(&opt, &b), 0);

  ct_export* e = nullptr;
  ASSERT_EQ(ct_snapshot_export_begin(a, 0, &e), 0);
  ct_import* im = nullptr;
  ASSERT_EQ(ct_snapshot_import_begin(b, &im), 0);
  while (true) {
    ct_buf chunk = {};
    int32_t done = 0;
    ASSERT_EQ(ct_snapshot_export_next(e, &chunk, &done), 0);
    if (chunk.len > 0) {
      ASSERT_EQ(ct_snapshot_import_feed(im, chunk.data, chunk.len), 0);
    }
    ct_free_buf(&chunk);
    if (done) {
      break;
    }
  }
  ct_snapshot_export_end(e);
  uint64_t at = 0;
  ASSERT_EQ(ct_snapshot_import_finish(im, &at), 0);
  ct_snapshot_import_end(im);
  EXPECT_EQ(at, 40u);

  for (int i = 0; i < 40; ++i) {
    int32_t found = 0;
    uint64_t slot = 0;
    ct_buf val = {};
    std::string k = Key(i);
    ASSERT_EQ(ct_get(b, reinterpret_cast<const uint8_t*>(k.data()), k.size(), &found, &slot, &val),
              0);
    ASSERT_EQ(found, 1) << "missing " << k;
    EXPECT_EQ(std::string(reinterpret_cast<char*>(val.data), val.len), "v" + std::to_string(i));
    ct_free_buf(&val);
  }
  ct_close(a);
  ct_close(b);
}
