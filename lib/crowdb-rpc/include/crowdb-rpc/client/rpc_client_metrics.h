// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Global RPC client counters (static, shared across all RpcClient
// instances). Uses crowdb-common::metrics::Counter for consistency with
// the rest of the codebase. Counters are registered with
// MetricsRegistry::global() so they appear in the periodic metrics log
// flush. Thread-safe (function-local statics, C++11+).
// Use reset_rpc_client_counters() at the start of a test to zero the
// window (flush() accumulates into total and resets window).
#pragma once

#include "crowdb-common/metrics/metrics.h"

namespace crowdb::rpc
{

using crowdb::common::metrics::Counter;
using crowdb::common::metrics::MetricsRegistry;

// 3 error counters (success path count is in the e2e histogram):
//   submit_retry   — coroutine submit failed then retried (co_client)
//   resp_missed    — on_response: late/dup/wrong_id/dropped
//   reaped         — reaper timed out (slab or map)
// Note: submit_fail is tracked by rpc.send.queue.full.c (transport layer).

Counter &rpc_submit_retry();
Counter &rpc_resp_missed();
Counter &rpc_reaped();

// Reset all window values to 0 (accumulates into total). Call at the
// start of a test to isolate counter deltas.
void reset_rpc_client_counters();

} // namespace crowdb::rpc
