// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! RPC bench target: provisions an in-process `RpcServer` with a
//! built-in echo handler, builds `RpcClient`-backed workers that
//! send ping requests with data payloads and verify the echo response.
//! Implemented in Phase 3.
