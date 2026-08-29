// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/metrics/c_api.h"

#include "crowdb-common/metrics/metrics.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

extern "C" char *crowdb_common_metrics_global_flush(double window_secs, const char *timestamp,
                                                    const char *section_label, size_t width, size_t count_w,
                                                    size_t tps_w)
{
    if (timestamp == nullptr) {
        return nullptr;
    }
    auto  &reg = crowdb::common::metrics::MetricsRegistry::global();
    char  *buf = nullptr;
    size_t len = 0;
    FILE  *fp  = open_memstream(&buf, &len);
    if (fp == nullptr) {
        return nullptr;
    }
    const char *label = section_label != nullptr ? section_label : "cpp-metrics-global";
    reg.flush_to(fp, window_secs, timestamp, label, width, count_w, tps_w);
    std::fflush(fp);
    std::fclose(fp);
    if (len == 0) {
        free(buf);
        return nullptr;
    }
    char *out = static_cast<char *>(std::malloc(len + 1));
    if (out == nullptr) {
        free(buf);
        return nullptr;
    }
    std::memcpy(out, buf, len);
    out[len] = '\0';
    free(buf);
    return out;
}

extern "C" size_t crowdb_common_metrics_global_max_name_len(void)
{
    return crowdb::common::metrics::MetricsRegistry::global().max_name_len();
}

extern "C" void crowdb_common_metrics_global_free(char *s)
{
    std::free(s);
}
