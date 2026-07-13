// plan-tree #11 Phase 1: Reactor (io_uring event loop) + FileAsyncPageStore.
// Only built when CMake found liburing (see CMakeLists.txt's
// CROWTREE_HAVE_LIBURING gate) -- io_uring is Linux-only.
#include "crowtree/async_page_store.h"
#include "crowtree/reactor.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace crowtree;

namespace
{
std::string temp_path()
{
    std::array<char, 24> tmpl{"/tmp/crowtree_rx_XXXXXX"};
    int                  fd = mkstemp(tmpl.data());
    if (fd >= 0) {
        close(fd);
    }
    return tmpl.data();
}

// Bounded poll for a background-thread-set flag, matching the style already
// used for the background flush/GC threads (background_flush_test.cpp,
// gc_test.cpp): no condvar wiring for the test's own synchronization, just
// a short sleep loop with a generous overall deadline.
template <typename Pred> bool wait_for(Pred pred, int max_iters = 200, int sleep_ms = 5)
{
    for (int i = 0; i < max_iters; ++i) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    }
    return pred();
}
} // namespace

TEST(Reactor, SubmitReadCompletesViaCallback)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    std::vector<uint8_t> expected{10, 20, 30, 40, 50, 60, 70, 80};
    ASSERT_EQ(::pwrite(fd, expected.data(), expected.size(), 0), static_cast<ssize_t>(expected.size()));

    Reactor              r;
    std::atomic<bool>    done{false};
    std::atomic<int>     got_res{-1};
    std::vector<uint8_t> buf(expected.size(), 0);
    r.submit_read(fd, buf.data(), buf.size(), 0, [&](int res) {
        got_res.store(res, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });

    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_EQ(got_res.load(), static_cast<int>(expected.size()));
    EXPECT_EQ(buf, expected);

    ::close(fd);
    std::remove(path.c_str());
}

TEST(Reactor, SubmitWriteThenReadRoundTrips)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);

    Reactor              r;
    std::atomic<bool>    done{false};
    std::atomic<int>     got_res{-1};
    std::vector<uint8_t> in{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    r.submit_write(fd, in.data(), in.size(), 100, [&](int res) {
        got_res.store(res, std::memory_order_relaxed);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_EQ(got_res.load(), static_cast<int>(in.size()));

    // Confirms the reactor thread actually performed the write (not just
    // invoked the callback with a fabricated success) -- read back with a
    // plain pread, bypassing the reactor entirely.
    std::vector<uint8_t> out(in.size(), 0);
    ASSERT_EQ(::pread(fd, out.data(), out.size(), 100), static_cast<ssize_t>(out.size()));
    EXPECT_EQ(out, in);

    ::close(fd);
    std::remove(path.c_str());
}

TEST(Reactor, MultipleConcurrentSubmitsAllComplete)
{
    constexpr int kOps = 64;
    std::string   path = temp_path();
    int           fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::ftruncate(fd, static_cast<off_t>(kOps) * 16), 0);

    Reactor          r;
    std::atomic<int> completed{0};
    std::vector<int> results(kOps, -1);
    // Each op writes a distinct 16-byte pattern at a distinct offset so a
    // wrong callback<->op mapping (e.g. always invoking op 0's callback)
    // shows up as either a missing completion or wrong bytes on read-back.
    std::vector<std::vector<uint8_t>> patterns(kOps);
    for (int i = 0; i < kOps; ++i) {
        patterns[i].assign(16, static_cast<uint8_t>(i));
    }
    for (int i = 0; i < kOps; ++i) {
        r.submit_write(fd, patterns[i].data(), patterns[i].size(), static_cast<off_t>(i) * 16, [&, i](int res) {
            results[i] = res;
            completed.fetch_add(1, std::memory_order_acq_rel);
        });
    }

    ASSERT_TRUE(wait_for([&] { return completed.load(std::memory_order_acquire) == kOps; }, /*max_iters=*/400));
    for (int i = 0; i < kOps; ++i) {
        EXPECT_EQ(results[i], 16) << "op " << i;
    }

    for (int i = 0; i < kOps; ++i) {
        std::vector<uint8_t> out(16, 0);
        ASSERT_EQ(::pread(fd, out.data(), out.size(), static_cast<off_t>(i) * 16), 16);
        EXPECT_EQ(out, patterns[i]) << "op " << i;
    }

    ::close(fd);
    std::remove(path.c_str());
}

TEST(Reactor, CancelBeforeCompletionSuppressesCallback)
{
    std::string path = temp_path();
    int         fd   = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    // A larger buffer than the other tests widens the window between kernel
    // completion and the reactor thread's dispatch, making the immediately-
    // following cancel() reliably win the race in practice (see class
    // comment on Reactor::cancel / design §8 for why this is best-effort,
    // not a hard guarantee, in general).
    std::vector<uint8_t> buf(1 << 16, 0);
    ASSERT_EQ(::ftruncate(fd, static_cast<off_t>(buf.size())), 0);

    Reactor           r;
    std::atomic<bool> fired{false};
    uint64_t          op_id =
        r.submit_read(fd, buf.data(), buf.size(), 0, [&](int) { fired.store(true, std::memory_order_release); });
    r.cancel(op_id);

    // Bounded wait: give the reactor plenty of opportunity to (wrongly)
    // dispatch before concluding it never will.
    EXPECT_FALSE(wait_for([&] { return fired.load(std::memory_order_acquire); }, /*max_iters=*/40));

    ::close(fd);
    std::remove(path.c_str());
}

TEST(Reactor, DestructorStopsThreadCleanly)
{
    {
        Reactor r;
        EXPECT_GE(r.eventfd(), 0);
    }
    SUCCEED();
}

// ── FileAsyncPageStore (this phase's PageStore twin) ───────────────

TEST(FileAsyncPageStore, WriteThenReadRoundTrips)
{
    std::string                         path = temp_path();
    Reactor                             r;
    std::unique_ptr<FileAsyncPageStore> s;
    ASSERT_TRUE(FileAsyncPageStore::open(path, 4096, &r, &s).ok());

    std::vector<uint8_t> in{9, 8, 7, 6, 5};
    std::atomic<bool>    write_done{false};
    Status               write_status;
    s->submit_write(200, in.data(), in.size(), [&](Status st) {
        write_status = std::move(st);
        write_done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return write_done.load(std::memory_order_acquire); }));
    EXPECT_TRUE(write_status.ok()) << write_status.to_string();

    std::vector<uint8_t> out(in.size(), 0);
    std::atomic<bool>    read_done{false};
    Status               read_status;
    s->submit_read(200, out.data(), out.size(), [&](Status st) {
        read_status = std::move(st);
        read_done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return read_done.load(std::memory_order_acquire); }));
    EXPECT_TRUE(read_status.ok()) << read_status.to_string();
    EXPECT_EQ(out, in);

    std::remove(path.c_str());
}

TEST(FileAsyncPageStore, ReadPastEndSurfacesAsError)
{
    std::string                         path = temp_path();
    Reactor                             r;
    std::unique_ptr<FileAsyncPageStore> s;
    ASSERT_TRUE(FileAsyncPageStore::open(path, 4096, &r, &s).ok());

    std::vector<uint8_t> out(16, 0);
    std::atomic<bool>    done{false};
    Status               status;
    s->submit_read(0, out.data(), out.size(), [&](Status st) {
        status = std::move(st);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_FALSE(status.ok());

    std::remove(path.c_str());
}

TEST(FileAsyncPageStore, FsyncCompletes)
{
    std::string                         path = temp_path();
    Reactor                             r;
    std::unique_ptr<FileAsyncPageStore> s;
    ASSERT_TRUE(FileAsyncPageStore::open(path, 4096, &r, &s).ok());

    std::atomic<bool> done{false};
    Status            status;
    Status            submit_status = s->submit_fsync([&](Status st) {
        status = std::move(st);
        done.store(true, std::memory_order_release);
    });
    ASSERT_TRUE(submit_status.ok());
    ASSERT_TRUE(wait_for([&] { return done.load(std::memory_order_acquire); }));
    EXPECT_TRUE(status.ok()) << status.to_string();

    std::remove(path.c_str());
}
