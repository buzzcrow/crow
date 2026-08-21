// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/client/rpc_client_metrics.h"

namespace crow::rpc
{

// Each function-local static registers with MetricsRegistry::global()
// so the counters appear in the periodic metrics log flush. The Counter
// is owned by the registry (unique_ptr); we store the raw pointer for
// hot-path access.

Counter &rpc_submit_ok()
{
    static Counter *c = MetricsRegistry::global().register_counter("rpc.client.submit_ok.c");
    return *c;
}

Counter &rpc_submit_fail()
{
    static Counter *c = MetricsRegistry::global().register_counter("rpc.client.submit_fail.c");
    return *c;
}

Counter &rpc_resp_matched()
{
    static Counter *c = MetricsRegistry::global().register_counter("rpc.client.resp_matched.c");
    return *c;
}

Counter &rpc_resp_missed()
{
    static Counter *c = MetricsRegistry::global().register_counter("rpc.client.resp_missed.c");
    return *c;
}

Counter &rpc_reaped()
{
    static Counter *c = MetricsRegistry::global().register_counter("rpc.client.reaped.c");
    return *c;
}

Counter &rpc_slab_fallback()
{
    static Counter *c = MetricsRegistry::global().register_counter("rpc.client.slab_fallback.c");
    return *c;
}

void reset_rpc_client_counters()
{
    rpc_submit_ok().flush();
    rpc_submit_fail().flush();
    rpc_resp_matched().flush();
    rpc_resp_missed().flush();
    rpc_reaped().flush();
    rpc_slab_fallback().flush();
}

} // namespace crow::rpc
