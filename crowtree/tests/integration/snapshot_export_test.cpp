// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// PT7: snapshot export / import (portable stream + file wrappers).
#include "crowtree/crowtree.h"
#include "crowtree/snapshot_io.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace crowtree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

Batch del_one(const std::string &k)
{
    return Batch{{{.key = k, .kind = OpKind::kDelete, .value = ""}}};
}

std::string make_key(int i)
{
    std::array<char, 16> buf{};
    snprintf(buf.data(), buf.size(), "key%05d", i);
    return buf.data();
}

// build a tree with puts, overwrites, and deletes (so the snapshot carries
// tombstones). Returns the live-key oracle.
void build_source(Crowtree *t, std::map<std::string, std::string> *live)
{
    uint64_t slot = 0;
    for (int i = 0; i < 120; ++i) {
        ++slot;
        ASSERT_TRUE(t->apply(slot, put_one(make_key(i), "v" + std::to_string(i))).ok());
        (*live)[make_key(i)] = "v" + std::to_string(i);
        if (i % 11 == 0) {
            ASSERT_TRUE(t->flush().ok());
        }
    }
    // Overwrite some, delete some.
    for (int i = 0; i < 120; i += 7) {
        ++slot;
        ASSERT_TRUE(t->apply(slot, put_one(make_key(i), "updated" + std::to_string(i))).ok());
        (*live)[make_key(i)] = "updated" + std::to_string(i);
    }
    for (int i = 3; i < 120; i += 13) {
        ++slot;
        ASSERT_TRUE(t->apply(slot, del_one(make_key(i))).ok());
        live->erase(make_key(i));
    }
    ASSERT_TRUE(t->flush().ok());
}

// Stream A -> B through small chunks (forces multi-chunk transfer).
void transfer(Crowtree &a, Crowtree &b, size_t chunk_bytes, uint64_t *at_slot,
              snapshot_format fmt = snapshot_format::kPortable)
{
    std::unique_ptr<SnapshotExport> exp;
    ASSERT_TRUE(snapshot_export_begin(a, fmt, chunk_bytes, &exp).ok());
    SnapshotImport imp(b);
    bool           done = false;
    while (!done) {
        std::string chunk;
        ASSERT_TRUE(exp->next_chunk(&chunk, &done).ok());
        ASSERT_TRUE(imp.feed(Slice(chunk)).ok());
    }
    ASSERT_TRUE(imp.finish(at_slot).ok());
}
} // namespace

TEST(SnapshotExport, ExportImportCompareEmpty)
{
    Options                            opt; // pure in-memory engines
    Crowtree                           a(opt);
    std::map<std::string, std::string> live;
    build_source(&a, &live);

    Crowtree b(opt);
    uint64_t at = 0;
    transfer(a, b, 4096, &at);
    EXPECT_EQ(at, a.last_applied_slot());
    EXPECT_EQ(b.last_applied_slot(), a.last_applied_slot());

    // Structural compare (including tombstones) must be identical.
    auto sa = a.snapshot_view();
    auto sb = b.snapshot_view();
    EXPECT_TRUE(sa->compare(*sb).empty());
    EXPECT_EQ(sa->size(), sb->size());
}

TEST(SnapshotExport, CrossEngineParityVsOracle)
{
    Options                            opt;
    Crowtree                           a(opt);
    std::map<std::string, std::string> live;
    build_source(&a, &live);

    Crowtree b(opt);
    uint64_t at = 0;
    transfer(a, b, 8192, &at);

    // Every live key reads back; deleted keys are gone.
    for (const auto &kv : live) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(b.get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
    std::string v;
    uint64_t    s;
    EXPECT_FALSE(b.get(Slice(make_key(3)), &s, &v)); // deleted in BuildSource
}

// #13: install_snapshot must be safe against concurrent lock-free readers. B
// starts with a populated multi-level tree; while reader threads walk it, we
// repeatedly import A's snapshot into B (each import epoch-retires B's old tree).
// A UAF in free_subtree would trip ASan/TSan here.
TEST(SnapshotExport, ConcurrentReadersDuringImportNoUAF)
{
    Options                            opt;
    Crowtree                           a(opt);
    std::map<std::string, std::string> live;
    build_source(&a, &live);

    Crowtree b(opt);
    {
        std::map<std::string, std::string> tmp;
        build_source(&b, &tmp); // B has its own multi-level tree to be replaced
    }

    std::atomic<bool>        stop{false};
    std::atomic<uint64_t>    reads{0};
    std::vector<std::thread> readers;
    readers.reserve(4);
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                for (int k = 0; k < 120; ++k) {
                    std::string v;
                    uint64_t    s;
                    (void)b.get(Slice(make_key(k)), &s, &v); // transient miss OK; must not UAF
                    reads.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (int round = 0; round < 5; ++round) {
        uint64_t at = 0;
        transfer(a, b, 4096, &at);
    }
    stop.store(true);
    for (auto &t : readers) {
        t.join();
    }
    EXPECT_GT(reads.load(), 0U);

    // After the churn settles, B matches the source oracle.
    for (const auto &kv : live) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(b.get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
}

TEST(SnapshotExport, FileDumpLoadRoundTrip)
{
    Options                            opt;
    Crowtree                           a(opt);
    std::map<std::string, std::string> live;
    build_source(&a, &live);

    crowtree_test::TempFile tmp("snap_");
    ASSERT_FALSE(tmp.path.empty());
    std::string path = tmp.path;

    ASSERT_TRUE(snapshot_dump_to_file(a, snapshot_format::kPortable, path).ok());

    Crowtree b(opt);
    ASSERT_TRUE(snapshot_load_from_file(b, path).ok());
    EXPECT_EQ(b.last_applied_slot(), a.last_applied_slot());

    auto sa = a.snapshot_view();
    auto sb = b.snapshot_view();
    EXPECT_TRUE(sa->compare(*sb).empty());
    std::remove(path.c_str());
}

TEST(SnapshotExport, ChunkBoundaryDeterminism)
{
    Options                            opt;
    Crowtree                           a(opt);
    std::map<std::string, std::string> live;
    build_source(&a, &live);

    auto collect = [&](size_t cb) {
        std::vector<std::string>        chunks;
        std::unique_ptr<SnapshotExport> exp;
        EXPECT_TRUE(snapshot_export_begin(a, snapshot_format::kPortable, cb, &exp).ok());
        bool done = false;
        while (!done) {
            std::string c;
            EXPECT_TRUE(exp->next_chunk(&c, &done).ok());
            chunks.push_back(std::move(c));
        }
        return chunks;
    };

    std::vector<std::string> first  = collect(1024);
    std::vector<std::string> second = collect(1024);
    ASSERT_EQ(first.size(), second.size());
    EXPECT_GT(first.size(), 1U); // multiple chunks at 1 KiB
    for (size_t i = 0; i < first.size(); ++i) {
        EXPECT_EQ(first[i], second[i]) << "chunk " << i << " differs";
        if (i + 1 < first.size()) {
            EXPECT_EQ(first[i].size(), 1024U);
        }
    }
}

TEST(SnapshotExport, CrcTamperRejected)
{
    Options                            opt;
    Crowtree                           a(opt);
    std::map<std::string, std::string> live;
    build_source(&a, &live);

    std::unique_ptr<SnapshotExport> exp;
    ASSERT_TRUE(snapshot_export_begin(a, snapshot_format::kPortable, kSnapshotChunkBytes, &exp).ok());
    std::string stream;
    bool        done = false;
    while (!done) {
        std::string c;
        ASSERT_TRUE(exp->next_chunk(&c, &done).ok());
        stream += c;
    }
    ASSERT_GT(stream.size(), 64U);
    stream[40] = static_cast<char>(stream[40] ^ 0xff); // flip a byte in the tuple body

    Crowtree       b(opt);
    SnapshotImport imp(b);
    ASSERT_TRUE(imp.feed(Slice(stream)).ok());
    EXPECT_EQ(imp.finish(nullptr).code(), Code::kCorruption);
}

// plan-tree #16: native format (raw frame images, no cell decode/tuple
// encode) must round-trip identically to portable -- same live keys/values,
// same structural compare via snapshot_view().
TEST(SnapshotExport, NativeExportImportRoundTrip)
{
    Options                            opt;
    Crowtree                           a(opt);
    std::map<std::string, std::string> live;
    build_source(&a, &live);

    Crowtree b(opt);
    uint64_t at = 0;
    transfer(a, b, 4096, &at, snapshot_format::kNative);
    EXPECT_EQ(at, a.last_applied_slot());
    EXPECT_EQ(b.last_applied_slot(), a.last_applied_slot());

    auto sa = a.snapshot_view();
    auto sb = b.snapshot_view();
    EXPECT_TRUE(sa->compare(*sb).empty());
    EXPECT_EQ(sa->size(), sb->size());

    for (const auto &kv : live) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(b.get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
    std::string v;
    uint64_t    s;
    EXPECT_FALSE(b.get(Slice(make_key(3)), &s, &v)); // deleted in build_source
}

// Native and portable exports of the *same* source tree must be logically
// equivalent (both replaying into fresh trees produce the same tree state),
// even though the wire bytes differ entirely.
TEST(SnapshotExport, NativeEquivalentToPortable)
{
    Options                            opt;
    Crowtree                           a(opt);
    std::map<std::string, std::string> live;
    build_source(&a, &live);

    Crowtree b_native(opt);
    Crowtree c_portable(opt);
    uint64_t at_native   = 0;
    uint64_t at_portable = 0;
    transfer(a, b_native, 4096, &at_native, snapshot_format::kNative);
    transfer(a, c_portable, 4096, &at_portable, snapshot_format::kPortable);
    EXPECT_EQ(at_native, at_portable);

    auto sb = b_native.snapshot_view();
    auto sc = c_portable.snapshot_view();
    EXPECT_TRUE(sb->compare(*sc).empty());
    EXPECT_EQ(sb->size(), sc->size());
}

// A native export survives a round-trip through a new, otherwise-empty tree
// (the common new-member-install shape) with no residual state.
TEST(SnapshotExport, NativeEmptyTreeRoundTrip)
{
    Options  opt;
    Crowtree a(opt); // never written to -- exports just the empty root leaf
    Crowtree b(opt);
    uint64_t at = 123; // sentinel to prove it gets overwritten to 0
    transfer(a, b, kSnapshotChunkBytes, &at, snapshot_format::kNative);
    EXPECT_EQ(at, 0U);
    EXPECT_EQ(b.last_applied_slot(), 0U);
    auto sb = b.snapshot_view();
    EXPECT_EQ(sb->size(), 0U);
}

TEST(SnapshotExport, NativeCrcTamperRejected)
{
    Options                            opt;
    Crowtree                           a(opt);
    std::map<std::string, std::string> live;
    build_source(&a, &live);

    std::unique_ptr<SnapshotExport> exp;
    ASSERT_TRUE(snapshot_export_begin(a, snapshot_format::kNative, kSnapshotChunkBytes, &exp).ok());
    std::string stream;
    bool        done = false;
    while (!done) {
        std::string c;
        ASSERT_TRUE(exp->next_chunk(&c, &done).ok());
        stream += c;
    }
    ASSERT_GT(stream.size(), 64U);
    stream[40] = static_cast<char>(stream[40] ^ 0xff); // flip a byte in the frame body

    Crowtree       b(opt);
    SnapshotImport imp(b);
    ASSERT_TRUE(imp.feed(Slice(stream)).ok());
    EXPECT_EQ(imp.finish(nullptr).code(), Code::kCorruption);
}
