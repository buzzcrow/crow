// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/client/rpc_client_metrics.h"

namespace crow::rpc
{

// Each function-local static registers with MetricsRegistry::global()
// so the counters appear in the periodic metrics log flush. The Counter
// is owned by the registry (unique_ptr); we store the raw pointer for
// hot-path access.

Counter &rpc_submit_fail()
{
    static Counter *c = MetricsRegistry::global().register_counter("rpc.request.submit_fail.c");
    return *c;
}

Counter &rpc_resp_missed()
{
    static Counter *c = MetricsRegistry::global().register_counter("rpc.request.resp_missed.c");
    return *c;
}

Counter &rpc_reaped()
{
    static Counter *c = MetricsRegistry::global().register_counter("rpc.request.reaped.c");
    return *c;
}

void reset_rpc_client_counters()
{
    rpc_submit_fail().flush();
    rpc_resp_missed().flush();
    rpc_reaped().flush();
}

} // namespace crow::rpc
