// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// C ABI for flushing the global C++ MetricsRegistry. Used by the Rust
// MetricsRunner to include process-level C++ metrics (e.g. rpc.client.*)
// in the periodic metrics log alongside per-engine [cpp-metrics] blocks.
#pragma once

#include <cstddef>
#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

// Flush the global C++ MetricsRegistry to a malloc'd string.
// Returns nullptr if no metrics are registered or on error.
// Caller must free with crow_common_metrics_global_free().
// section_label is typically "cpp-metrics-global".
char *crow_common_metrics_global_flush(double window_secs, const char *timestamp, const char *section_label,
                                       size_t width, size_t count_w, size_t tps_w);

// Max metric name length in the global C++ registry (for column alignment).
size_t crow_common_metrics_global_max_name_len(void);

// Free a string returned by crow_common_metrics_global_flush.
void crow_common_metrics_global_free(char *s);

#ifdef __cplusplus
}
#endif
