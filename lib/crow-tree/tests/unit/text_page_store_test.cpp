// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Task 4: TextPageStore + text codec tests.
#include "crow-tree/debug_codec.h"
#include "crow-tree/text_codec.h"
#include "crow-tree/text_page_store.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

using namespace crow::tree;

namespace
{
std::string temp_dir()
{
    std::string root = crowtree_test::test_tmp_root();
    std::filesystem::create_directories(root);
    std::array<char, 128> tmpl{};
    std::snprintf(tmpl.data(), tmpl.size(), "%s/txt_XXXXXX", root.c_str());
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    char *d = mkdtemp(buf.data());
    if (d == nullptr) {
        return root + "/txt_fallback";
    }
    return d;
}

std::string read_file(const std::string &path)
{
    std::ifstream      ifs(path, std::ios::binary);
    std::ostringstream oss;
    oss << ifs.rdbuf();
    return oss.str();
}
} // namespace

// ── Text codec round-trip tests ───────────────────────────────────

TEST(TextCodec, AnchorRoundTrip)
{
    // Build a minimal anchor blob (60 bytes fixed fields + 4 CRC + padding)
    std::vector<uint8_t> anchor(64, 0);
    // magic = 0x41435443 (CTCA)
    anchor[0] = 0x43;
    anchor[1] = 0x54;
    anchor[2] = 0x43;
    anchor[3] = 0x41;
    // format_version = 2
    anchor[4] = 2;
    anchor[5] = 0;
    anchor[6] = 0;
    anchor[7] = 0;
    // snapshot_seq = 123
    anchor[8]  = 123;
    anchor[9]  = 0;
    anchor[10] = 0;
    anchor[11] = 0;
    anchor[12] = 0;
    anchor[13] = 0;
    anchor[14] = 0;
    anchor[15] = 0;
    // root_page_id = 42
    anchor[16] = 42;
    anchor[17] = 0;
    anchor[18] = 0;
    anchor[19] = 0;
    anchor[20] = 0;
    anchor[21] = 0;
    anchor[22] = 0;
    anchor[23] = 0;

    std::string text = encode_anchor_text(anchor.data(), anchor.size());
    EXPECT_NE(text.find("CROW_CT_ANCHOR"), std::string::npos);
    EXPECT_NE(text.find("snapshot_seq=123"), std::string::npos);
    EXPECT_NE(text.find("root_page_id=42"), std::string::npos);

    std::vector<uint8_t> decoded;
    ASSERT_TRUE(decode_anchor_text(text, &decoded).ok());
    EXPECT_EQ(anchor, decoded);
}

TEST(TextCodec, SegImageRoundTrip)
{
    // Build a minimal segment image blob
    std::vector<uint8_t> img(32, 0);
    // magic = 0x534D5443 (CTMS)
    img[0] = 0x43;
    img[1] = 0x54;
    img[2] = 0x4D;
    img[3] = 0x53;

    std::string text = encode_seg_image_text(img.data(), img.size());
    EXPECT_NE(text.find("CROW_CT_SEGIMG"), std::string::npos);

    std::vector<uint8_t> decoded;
    ASSERT_TRUE(decode_seg_image_text(text, &decoded).ok());
    EXPECT_EQ(img, decoded);
}

TEST(TextCodec, SegDirRoundTrip)
{
    // Build a minimal segment directory blob
    std::vector<uint8_t> dir(16, 0);
    // magic = 0x44535443 (CTSD)
    dir[0] = 0x43;
    dir[1] = 0x54;
    dir[2] = 0x53;
    dir[3] = 0x44;

    std::string text = encode_segdir_text(dir.data(), dir.size());
    EXPECT_NE(text.find("CROW_CT_SEGDIR"), std::string::npos);

    std::vector<uint8_t> decoded;
    ASSERT_TRUE(decode_segdir_text(text, &decoded).ok());
    EXPECT_EQ(dir, decoded);
}

TEST(TextCodec, AllOutputsAreHumanReadable)
{
    std::vector<uint8_t> blob(64, 0x41);
    blob[0] = 0x43;
    blob[1] = 0x54;
    blob[2] = 0x43;
    blob[3] = 0x41; // anchor magic

    std::string text = encode_anchor_text(blob.data(), blob.size());
    // No null bytes in the text output
    EXPECT_EQ(text.find('\0'), std::string::npos);
}

// ── TextPageStore tests ───────────────────────────────────────────

TEST(TextPageStore, WritePageCreatesFile)
{
    std::string                    base = temp_dir();
    std::unique_ptr<TextPageStore> s;
    ASSERT_TRUE(TextPageStore::open(base, 0, 0, &s).ok());

    // Write a page blob (no recognized magic → treated as page frame)
    std::vector<uint8_t> page_data(64, 0x42);
    ASSERT_TRUE(s->write_at(8192, page_data.data(), page_data.size()).ok());
    ASSERT_TRUE(s->sync().ok());

    std::string filename = s->dir() + "/page-8192.ck";
    std::string content  = read_file(filename);
    EXPECT_FALSE(content.empty());
    // Should be human-readable (debug_codec text format)
    EXPECT_EQ(content.find('\0'), std::string::npos);
}

TEST(TextPageStore, WriteAnchorCreatesFile)
{
    std::string                    base = temp_dir();
    std::unique_ptr<TextPageStore> s;
    ASSERT_TRUE(TextPageStore::open(base, 0, 0, &s).ok());

    // Write an anchor blob at addr 0 (slot A)
    std::vector<uint8_t> anchor(64, 0);
    anchor[0] = 0x43;
    anchor[1] = 0x54;
    anchor[2] = 0x43;
    anchor[3] = 0x41; // CTCA magic
    anchor[4] = 2;    // format_version

    ASSERT_TRUE(s->write_at(0, anchor.data(), anchor.size()).ok());
    ASSERT_TRUE(s->sync().ok());

    std::string content = read_file(s->dir() + "/anchor-A.ck");
    EXPECT_NE(content.find("CROW_CT_ANCHOR"), std::string::npos);
}

TEST(TextPageStore, RoundTripReopen)
{
    std::string          base = temp_dir();
    std::vector<uint8_t> page_data(64, 0x55);

    {
        std::unique_ptr<TextPageStore> s;
        ASSERT_TRUE(TextPageStore::open(base, 0, 0, &s).ok());
        ASSERT_TRUE(s->write_at(8192, page_data.data(), page_data.size()).ok());
        ASSERT_TRUE(s->sync().ok());
    }
    {
        std::unique_ptr<TextPageStore> s;
        ASSERT_TRUE(TextPageStore::open(base, 0, 0, &s).ok());
        std::vector<uint8_t> out(page_data.size(), 0);
        ASSERT_TRUE(s->read_at(8192, out.data(), out.size()).ok());
        EXPECT_EQ(page_data, out);
    }
}

TEST(TextPageStore, ManifestMapsMultipleBlobs)
{
    std::string                    base = temp_dir();
    std::unique_ptr<TextPageStore> s;
    ASSERT_TRUE(TextPageStore::open(base, 0, 0, &s).ok());

    // Write multiple page blobs
    std::vector<uint8_t> data1(32, 0x11);
    std::vector<uint8_t> data2(32, 0x22);
    ASSERT_TRUE(s->write_at(100, data1.data(), data1.size()).ok());
    ASSERT_TRUE(s->write_at(200, data2.data(), data2.size()).ok());
    ASSERT_TRUE(s->sync().ok());

    // Verify manifest file exists and has entries
    std::string manifest = read_file(s->dir() + "/manifest.ck");
    EXPECT_NE(manifest.find("addr=100"), std::string::npos);
    EXPECT_NE(manifest.find("addr=200"), std::string::npos);
}

TEST(TextPageStore, SizeReturnsMaxAddr)
{
    std::string                    base = temp_dir();
    std::unique_ptr<TextPageStore> s;
    ASSERT_TRUE(TextPageStore::open(base, 0, 0, &s).ok());

    std::vector<uint8_t> data(10, 0);
    ASSERT_TRUE(s->write_at(100, data.data(), data.size()).ok());
    EXPECT_EQ(s->size(), 110U);

    ASSERT_TRUE(s->write_at(200, data.data(), data.size()).ok());
    EXPECT_EQ(s->size(), 210U);
}

TEST(TextPageStore, IuIsAlways1)
{
    std::string                    base = temp_dir();
    std::unique_ptr<TextPageStore> s;
    ASSERT_TRUE(TextPageStore::open(base, 0, 0, &s).ok());
    EXPECT_EQ(s->iu_size(), 1U);
}
