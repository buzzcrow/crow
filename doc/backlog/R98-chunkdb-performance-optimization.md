<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R94: chunkdb — Performance Optimization

**Problem**: The RPC layer may become a bottleneck on high-throughput paths
(gRPC was replaced by crow-rpc; this evaluates whether crow-rpc itself is
sufficient). Allocation pipeline and topology cache management may need
optimization for large-scale deployments.

**Solution**: Evaluate and optimize the crow-rpc transport on hot paths
(gRPC was already replaced by crow-rpc via R115/R116/R117/R32). Optimize allocation pipeline for reduced latency. Tune topology
cache refresh and connection pooling. Profile and optimize critical code
paths.

**Scope**: Placeholder - detailed design to be refined before implementation.