// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/buffer.h"
#include "crow-rpc/framing.h"
#include "crow-rpc/transport.h"

#include <cstdint>

namespace crow::rpc
{

// ── Message layer: flatbuffer control encode/decode ───────────────
//
// The wire frame carries a flatbuffer control message + raw data. This
// layer provides encode/decode helpers for the common message types
// (ping, unknown) and a generic Request/Response wrapper that service
// handlers extend.
//
// Service-specific message types (diskio, consensus) live in their own
// crates and use the same Frame + Buffer structures — they just parse
// the control buffer with their own generated flatbuffer headers.

// Extract request_id + rpc_create_nano from a flatbuffer control
// message. All common messages (ConnectionPingRequest/Response,
// UnknownMessage) share the same layout for these two fields.
// Declared in framing.h, implemented here (needs flatbuffer generated headers).

// Build a ping request control buffer. Returns a pool-allocated Buffer
// with the serialized ConnectionPingRequest flatbuffer.
Buffer *build_ping_request(BufferPool *pool, uint64_t request_id, uint64_t rpc_create_nano);

// Build a ping response control buffer. Returns a pool-allocated Buffer
// with the serialized ConnectionPingResponse flatbuffer.
// response_create_nano is the server's steady_clock::now() at response
// build time (server clock domain — not used by client for latency).
Buffer *build_ping_response(BufferPool *pool, uint64_t request_id, uint64_t rpc_create_nano,
                            uint64_t response_create_nano);

// Build an unknown-message response control buffer.
Buffer *build_unknown_response(BufferPool *pool, uint64_t request_id, uint64_t rpc_create_nano,
                               uint64_t response_create_nano);

// Build a generic response control buffer for a custom msg_type. The
// caller provides the raw flatbuffer bytes (already serialized). This
// wraps them in a pool Buffer and sets up the Frame header.
Buffer *build_raw_control(BufferPool *pool, const uint8_t *data, uint32_t len);

// Build an OutFrame ready for transport->submit(). The caller provides
// the control Buffer (flatbuffer) and optional data Buffer. The header
// is filled with msg_type, msg_size, data_size, flags.
OutFrame *build_out_frame(uint64_t request_id, uint16_t msg_type, Buffer *control, Buffer *data, uint8_t flags = 0);

} // namespace crow::rpc
