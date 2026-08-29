<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R94: chunkdb — Performance Optimization

**Problem**: The RPC layer may become a bottleneck on high-throughput paths
(the legacy transport was replaced by crowdb-rpc; this evaluates whether crowdb-rpc itself is
sufficient). Allocation pipeline and topology cache management may need
optimization for large-scale deployments.

**Solution**: Evaluate and optimize the crowdb-rpc transport on hot paths
(the legacy transport was already replaced by crowdb-rpc via R115/R116/R117/R32). Optimize allocation pipeline for reduced latency. Tune topology
cache refresh and connection pooling. Profile and optimize critical code
paths.

**Scope**: Placeholder - detailed design to be refined before implementation.