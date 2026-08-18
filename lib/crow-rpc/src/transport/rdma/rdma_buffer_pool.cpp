// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// RdmaBufferPool implementation is in rdma_transport.cpp (same compilation
// unit for the RDMA gate). This file exists so the CMakeLists.txt
// rdma_buffer_pool.cpp entry has a source to compile when RDMA is enabled.
// On non-RDMA builds, both files are excluded from the source list.

#ifdef CROW_RPC_HAVE_RDMA
// The actual RdmaBufferPool methods are defined in rdma_transport.cpp.
// This file is intentionally empty — it exists only for the CMake source
// list symmetry. When the RDMA implementation is completed, the buffer
// pool code may be split into this file.
#endif
