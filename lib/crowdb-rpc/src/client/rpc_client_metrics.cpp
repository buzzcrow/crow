// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/client/rpc_client_metrics.h"

namespace crowdb::rpc
{

// Each function-local static registers with MetricsRegistry::global()
// so the counters appear in the periodic metrics log flush. The Counter
// is owned by the registry (unique_ptr); we store the raw pointer for
// hot-path access.

Counter &rpc_submit_retry()
{
    static Counter *c = MetricsRegistry::global().register_counter("rpc.request.submit_retry.c");
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
    rpc_submit_retry().flush();
    rpc_resp_missed().flush();
    rpc_reaped().flush();
}

} // namespace crowdb::rpc
