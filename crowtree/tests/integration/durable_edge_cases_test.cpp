// Edge-case durability: empty values, binary keys/values with NULs, an oversized
// key (heap fallback), and zero/boundary-sized overflow values — all through the
// compression + overflow + snapshot + reopen path.
#include "crowtree/crowtree.h"
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
Options EdgeOpts(PageStore* s) {
  Options o;
  o.page_store = s;
  o.compression = compress_algo::kLz4;
  o.frame_bytes = 4096;
  o.max_inline_value = 64;
  o.max_delta_len = 1;
  o.leaf_split_bytes = 512;
  return o;
}
void CheckAll(Crowtree* t, const std::map<std::string, std::string>& oracle) {
  for (const auto& kv : oracle) {
    std::string v;
    uint64_t s;
    ASSERT_TRUE(t->get(Slice(kv.first), &s, &v)) << "missing key of len " << kv.first.size();
    EXPECT_EQ(v, kv.second) << "value mismatch for key len " << kv.first.size();
  }
}
}  // namespace

TEST(DurableEdgeCases, EmptyAndBinaryValuesReopen) {
  MemPageStore store(1);
  Options opt = EdgeOpts(&store);

  std::map<std::string, std::string> oracle;
  uint64_t slot = 0;
  {
    Crowtree t(opt);
    // empty value (Put with ""), boundary at the inline threshold, binary data.
    std::vector<std::pair<std::string, std::string>> items = {
        {"empty", ""},
        {std::string("bin\0key", 7), std::string("val\0ue\0", 7)},
        {"exactly64", std::string(64, 'x')},     // == max_inline_value (inline)
        {"justover64", std::string(65, 'y')},    // > threshold -> overflow
        {"big", std::string(20000, 'z')},        // multi-frame overflow
        {"nul-value", std::string(5000, '\0')},  // overflow of NUL bytes
    };
    for (auto& it : items) {
      ++slot;
      ASSERT_TRUE(t.apply(slot, Put1(it.first, it.second)).ok());
      ASSERT_TRUE(t.flush().ok());
      oracle[it.first] = it.second;
    }
    ASSERT_TRUE(t.snapshot(nullptr).ok());
    CheckAll(&t, oracle);
  }
  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
  CheckAll(t2.get(), oracle);
}

TEST(DurableEdgeCases, OversizedKeyRejectedNormalKeysDurable) {
  MemPageStore store(1);
  Options opt = EdgeOpts(&store);

  // plan-tree #15: a key larger than max_key_size (default frame_bytes/2) is now
  // rejected at apply() as a caller bug, rather than heap-fell-back into an
  // oversized leaf page. The rejection is all-or-nothing and the tree stays
  // usable + durable for normal keys through snapshot + reopen.
  std::string huge_key(6000, 'k');  // > frame_bytes/2 (2048)
  std::map<std::string, std::string> oracle;
  {
    Crowtree t(opt);
    // A rejected write leaves no durable effect; the learner fills its slot as a
    // NoOp (force_advance_slot) so the contiguous frontier still progresses.
    EXPECT_EQ(t.apply(1, Put1(huge_key, "small-value")).code(), Code::kInvalidArgument);
    t.force_advance_slot(1);
    ASSERT_TRUE(t.apply(2, Put1("normal", "v")).ok());
    ASSERT_TRUE(t.flush().ok());
    oracle["normal"] = "v";
    ASSERT_TRUE(t.snapshot(nullptr).ok());
    CheckAll(&t, oracle);
    // The rejected key is absent.
    std::string v;
    uint64_t s;
    EXPECT_FALSE(t.get(Slice(huge_key), &s, &v));
  }
  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
  CheckAll(t2.get(), oracle);
}

TEST(DurableEdgeCases, OverflowChunkBoundarySizes) {
  MemPageStore store(1);
  Options opt = EdgeOpts(&store);

  // Values exactly at, one below, and one above an overflow chunk boundary.
  const uint32_t cap = overflow_chunk_cap(opt.frame_bytes);  // payload per frame
  std::map<std::string, std::string> oracle;
  {
    Crowtree t(opt);
    std::vector<size_t> sizes = {cap - 1, cap, cap + 1, 2 * cap, 2 * cap + 1, 3 * cap};
    uint64_t slot = 0;
    for (size_t i = 0; i < sizes.size(); ++i) {
      ++slot;
      std::string key = "k" + std::to_string(i);
      std::string v(sizes[i], static_cast<char>('A' + static_cast<int>(i)));
      ASSERT_TRUE(t.apply(slot, Put1(key, v)).ok());
      ASSERT_TRUE(t.flush().ok());
      oracle[key] = v;
    }
    ASSERT_TRUE(t.snapshot(nullptr).ok());
    CheckAll(&t, oracle);
  }
  std::unique_ptr<Crowtree> t2;
  ASSERT_TRUE(Crowtree::open(opt, &t2).ok());
  CheckAll(t2.get(), oracle);
}
