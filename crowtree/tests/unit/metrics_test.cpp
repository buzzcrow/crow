// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crowtree/metrics.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <string>

namespace crowtree
{
namespace
{

TEST(MetricsCounter, WindowResetAndTotalAccumulate)
{
    Counter c("test.c");
    c.inc();
    c.inc();
    auto snap = c.flush();
    EXPECT_EQ(snap.count, 2u);
    EXPECT_EQ(snap.total, 2u);

    c.inc();
    snap = c.flush();
    EXPECT_EQ(snap.count, 1u);
    EXPECT_EQ(snap.total, 3u);

    snap = c.flush();
    EXPECT_EQ(snap.count, 0u);
    EXPECT_EQ(snap.total, 3u);
}

TEST(MetricsGauge, ReportsLastValue)
{
    Gauge g("test.g");
    g.set(42);
    EXPECT_EQ(g.get(), 42u);
    g.set(0);
    EXPECT_EQ(g.get(), 0u);
}

TEST(MetricsBandwidth, BasicFlush)
{
    Bandwidth bw("test.bw");
    for (int i = 0; i < 10; ++i) {
        bw.observe(100);
    }
    auto snap = bw.flush();
    EXPECT_EQ(snap.count, 10u);
    EXPECT_EQ(snap.sum, 1000u);
    EXPECT_EQ(snap.total_bytes, 1000u);

    snap = bw.flush();
    EXPECT_EQ(snap.count, 0u);
    EXPECT_EQ(snap.total_bytes, 1000u);
}

TEST(MetricsHistogram, P50P99WithKnownDistribution)
{
    LatencyHistogram h("test.lh");
    for (int i = 0; i < 100; ++i) {
        h.observe(500'000); // 500us
    }
    auto snap = h.flush();
    EXPECT_EQ(snap.count, 100u);
    auto p50 = LatencyHistogram::percentile(snap, 50.0);
    auto p99 = LatencyHistogram::percentile(snap, 99.0);
    EXPECT_EQ(p50, 500'000u);
    EXPECT_EQ(p99, 500'000u);
}

TEST(MetricsSummary, AvgAndMax)
{
    LatencySummary s("test.ls");
    s.observe(100);
    s.observe(200);
    s.observe(300);
    auto snap = s.flush();
    EXPECT_EQ(snap.count, 3u);
    EXPECT_EQ(snap.sum, 600u);
    EXPECT_EQ(snap.max, 300u);
    EXPECT_EQ(snap.total_count, 3u);

    uint64_t avg = snap.sum / snap.count;
    EXPECT_EQ(avg, 200u);
}

TEST(MetricsSummary, MaxResetsAfterFlush)
{
    LatencySummary s("test.ls2");
    s.observe(500);
    auto snap = s.flush();
    EXPECT_EQ(snap.max, 500u);

    snap = s.flush();
    EXPECT_EQ(snap.max, 0u);
}

TEST(MetricsRegistry, RegisterReturnsUsableHandle)
{
    MetricsRegistry reg;
    auto           *c = reg.register_counter("s.1.test.c");
    ASSERT_NE(c, nullptr);
    c->inc();
    c->inc();

    auto *g = reg.register_gauge("s.1.test.g");
    ASSERT_NE(g, nullptr);
    g->set(99);

    auto *bw = reg.register_bandwidth("s.1.test.bw");
    ASSERT_NE(bw, nullptr);
    bw->observe(42);

    auto *h = reg.register_histogram("s.1.test.lh");
    ASSERT_NE(h, nullptr);
    h->observe(1'000'000);

    auto *s = reg.register_summary("s.1.test.ls");
    ASSERT_NE(s, nullptr);
    s->observe(1'000'000);
}

TEST(MetricsRegistry, FlushFormat)
{
    MetricsRegistry reg;
    auto           *c = reg.register_counter("s.1.kv.delete.c");
    c->inc();
    c->inc();

    auto *g = reg.register_gauge("s.1.g.0.buf.resident.g");
    g->set(512);

    auto *s = reg.register_summary("s.1.kv.scan.l");
    s->observe(1'200'000);
    s->observe(800'000);

    std::string tmp = "/tmp/crowtree_metrics_test_XXXXXX";
    FILE       *fp  = tmpfile();
    ASSERT_NE(fp, nullptr);
    reg.flush_to(fp, 5.0, "2026-07-15T16:30:05.123Z");
    std::fflush(fp);

    // Read back via rewind + fread
    std::rewind(fp);
    char   buf[4096];
    size_t n = std::fread(buf, 1, sizeof(buf) - 1, fp);
    buf[n]   = '\0';
    std::fclose(fp);

    std::string output(buf);
    EXPECT_NE(output.find("[metrics"), std::string::npos);
    EXPECT_NE(output.find("s.1.kv.delete.c"), std::string::npos);
    EXPECT_NE(output.find("s.1.g.0.buf.resident.g"), std::string::npos);
    EXPECT_NE(output.find("s.1.kv.scan.l"), std::string::npos);
    EXPECT_NE(output.find("512"), std::string::npos);
}

} // namespace
} // namespace crowtree
