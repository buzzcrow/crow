// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Global RPC client counters (static, shared across all RpcClient
// instances). Uses crow-common::metrics::Counter for consistency with
// the rest of the codebase. Counters are function-local statics
// (Meyers singletons) — thread-safe in C++11+, initialized on first
// access. Use reset_rpc_client_counters() at the start of a test to
// zero the window (flush() accumulates into total and resets window).
#pragma once

#include "crow-common/metrics/counter.h"

namespace crow::rpc
{

using crow::common::metrics::Counter;

// 6 global counters (reduced from 12 per-instance atomics):
//   submit_ok      — send() succeeded (slab or map)
//   submit_fail    — send() submit failed
//   resp_matched   — on_response matched (slab or map)
//   resp_missed    — on_response: late/dup/wrong_id/dropped
//   reaped         — reaper timed out (slab or map)
//   slab_fallback  — send() fell back to map (slab slot occupied)

Counter &rpc_submit_ok();
Counter &rpc_submit_fail();
Counter &rpc_resp_matched();
Counter &rpc_resp_missed();
Counter &rpc_reaped();
Counter &rpc_slab_fallback();

// Reset all window values to 0 (accumulates into total). Call at the
// start of a test to isolate counter deltas.
void reset_rpc_client_counters();

} // namespace crow::rpc
