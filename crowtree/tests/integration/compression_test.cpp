// PT10.3: end-to-end page compression. checkpoint writes compressed durable
// blobs; reopen/demand-load/eviction decode them transparently; a tampered
// stored byte fails CRC on reload.
#include "crowtree/compressor.h"
#include "crowtree/crowtree.h"
#include "crowtree/env.h"
#include "crowtree/page_store.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace crowtree;

namespace {
Batch Put1(const std::string& k, const std::string& v) {
  return Batch{{batch_op{k, OpKind::kPut, v}}};
}
std::string Key(int i) {
  char buf[16];
  snprintf(buf, sizeof(buf), "key%05d", i);
  return buf;
}
// Highly compressible payload so LZ4 actually shrinks pages when available.
std::string Val(int i) { return "value-" + std::to_string(i) + std::string(64, 'a'); }
void Fill(Crowtree* t, int K, std::map<std::string, std::string>* oracle) {
  for (int i = 0; i < K; ++i) {
    std::string v = Val(i);
    ASSERT_TRUE(t->apply(i + 1, Put1(Key(i), v)).ok());
    ASSERT_TRUE(t->flush().ok());
    (*oracle)[Key(i)] = v;
  }
}
}  // namespace

TEST(Compression, CheckpointReopenWithCompressedPages) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  opt.compression = compress_algo::kLz4;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 200;  // multi-level tree -> many pages
  opt.frame_bytes = 4096;
  CrowtreeEnv env;

  std::map<std::string, std::string> oracle;
  uint64_t compressed_size = 0;
  {
    Crowtree t(env, opt);
    Fill(&t, 200, &oracle);
    ASSERT_GT(t.height(), 1);
    ASSERT_TRUE(t.checkpoint(nullptr).ok());
    compressed_size = store.size();
  }

  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::open(env, opt, &t2).ok());
  EXPECT_GT(t2->height(), 1);
  for (const auto& kv : oracle) {
    std::string v;
    uint64_t s;
    ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
    EXPECT_EQ(v, kv.second);
  }

  // When LZ4 is linked, the compressed image should be smaller than an
  // uncompressed checkpoint of the same tree.
  if (lz4_available()) {
    MemPageStore raw_store(1);
    Options raw_opt = opt;
    raw_opt.page_store = &raw_store;
    raw_opt.compression = compress_algo::kNone;
    CrowtreeEnv raw_env;
    Crowtree raw(raw_env, raw_opt);
    std::map<std::string, std::string> raw_oracle;
    Fill(&raw, 200, &raw_oracle);
    ASSERT_TRUE(raw.checkpoint(nullptr).ok());
    EXPECT_LT(compressed_size, raw_store.size());
  }
}

TEST(Compression, EvictionReloadOfCompressedPages) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  opt.compression = compress_algo::kLz4;
  opt.max_delta_len = 1;
  opt.leaf_split_bytes = 200;
  opt.frame_bytes = 4096;
  CrowtreeEnv env;
  Crowtree t(env, opt);

  std::map<std::string, std::string> oracle;
  Fill(&t, 200, &oracle);
  ASSERT_TRUE(t.checkpoint(nullptr).ok());

  // Force all clean leaves out of the pool, then read everything back: each
  // access demand-loads + decompresses the durable blob.
  t.evict_clean_leaves(0);
  for (const auto& kv : oracle) {
    std::string v;
    uint64_t s;
    ASSERT_TRUE(t.get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
    EXPECT_EQ(v, kv.second);
  }
}

TEST(Compression, CrcTamperRejectedOnReopen) {
  MemPageStore store(1);
  Options opt;
  opt.page_store = &store;
  opt.compression = compress_algo::kLz4;
  opt.frame_bytes = 4096;
  CrowtreeEnv env;
  {
    Crowtree t(env, opt);
    ASSERT_TRUE(t.apply(1, Put1("a", Val(1))).ok());
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.checkpoint(nullptr).ok());
  }

  // Corrupt a page's stored bytes deep in the page region (past both 4 KiB
  // superblock slots). The blob CRC must reject it on demand-load.
  uint64_t off = 2 * 4096 + 64;  // inside the first page's stored area
  uint8_t b = 0;
  ASSERT_TRUE(store.read_at(off, &b, 1).ok());
  b ^= 0xff;
  ASSERT_TRUE(store.write_at(off, &b, 1).ok());

  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::open(env, opt, &t2).ok());  // lazy: tags only
  // Demand-load fails CRC -> the page reads as a miss, but the media fault is
  // latched so a caller can detect the corruption.
  std::string v;
  uint64_t s;
  EXPECT_FALSE(t2->get(Slice("a"), &s, &v));
  EXPECT_TRUE(t2->io_failed());
}
