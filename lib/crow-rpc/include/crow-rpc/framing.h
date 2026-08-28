// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crow-rpc/buffer.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <vector>

namespace crow::rpc
{

// ── Wire format (14-byte header) ──────────────────────────────────
//
// [magic:2][msg_type:2][msg_size:4][data_size:4][msg_offset:1][flags:1]
//
// Little-endian, field-by-field (not memcpy of the struct — avoids
// compiler-layout dependence). See design-crow-rpc.md §3 for
// the rationale behind each field and what was removed from the
// reference's 20-byte header.

constexpr uint16_t MAGIC       = 0xCA70;
constexpr uint8_t  HEADER_SIZE = 14;

// flags bit definitions
constexpr uint8_t FLAG_ONE_WAY = 0x01;

struct Header
{
    uint16_t magic      = MAGIC;
    uint16_t msg_type   = 0;
    uint32_t msg_size   = 0;           // control message length
    uint32_t data_size  = 0;           // data payload length
    uint8_t  msg_offset = HEADER_SIZE; // offset to control message
    uint8_t  flags      = 0;
};

// A complete frame: header + extracted control fields + optional data
// buffer. The parser extracts request_id + rpc_create_nano from the
// flatbuffer control message during parse and discards
// the control bytes. Data is read into a pool-allocated, ref-counted
// Buffer. The handler/callback that receives a Frame* must delete it
// when done (destructor releases data_buf to the pool).
struct Frame
{
    Header   header;
    uint64_t request_id           = 0;       // extracted from control during parse
    uint64_t rpc_create_nano      = 0;       // extracted from control during parse
    uint64_t response_create_nano = 0;       // extracted from control during parse (responses only)
    Buffer  *data_buf             = nullptr; // pool-allocated; nullptr if control-only

    // Raw control message bytes (flatbuffer). Populated for all frames with
    // msg_size > 0. Common handlers (ping, unknown) use only request_id +
    // rpc_create_nano and ignore this; service-specific handlers (diskio)
    // parse the full flatbuffer from here.
    std::vector<uint8_t> control;

    ~Frame()
    {
        if (data_buf != nullptr) {
            data_buf->release();
        }
    }
};

enum class FramingError {
    None = 0,
    BadMagic,
    BadOffset,
    DataTooLarge,
};

// Serialize a header into a 12-byte buffer (little-endian, field-by-field).
void serialize_header(uint8_t *buf, const Header &h);

// Parse a 12-byte buffer into a header (little-endian, field-by-field).
Header parse_header(const uint8_t *buf);

// Extract request_id + rpc_create_nano + response_create_nano from a
// flatbuffer control message. All common messages
// (ConnectionPingRequest/Response, UnknownMessage) share the same
// layout for these fields. response_create_nano is 0 for requests.
void extract_control_fields(const uint8_t *control, uint32_t len, uint64_t &out_request_id,
                            uint64_t &out_rpc_create_nano, uint64_t &out_response_create_nano);

// ── FrameParser — pull-based zero-copy state machine ──────────────
//
// The parser drives receive-side zero-copy. Instead of reading into a
// scratch buffer and copying, it tells the read loop *where to read
// next* — directly into pool-allocated Buffers. This unifies the TCP and
// RDMA receive paths.
//
// Control fields (request_id, rpc_create_nano) are
// extracted during parse and the control bytes are discarded. Data is
// read into a pool-allocated Buffer. No malloc per frame.
//
// Usage in the read loop:
//   target = parser.next_read_target();
//   n = read(fd, target.ptr, target.len);
//   frame = parser.advance(n);
//   if (frame) dispatch(frame);
//
// For batched reads (header + control from recv_buf):
//   consumed = parser.feed_data(recv_buf, n, on_frame);
//   // feed_data stops at ReadingData — read loop handles data directly.

enum class ParseState {
    ReadingHeader,
    ReadingControl,
    ReadingData,
};

class FrameParser
{
  public:
    struct ReadTarget
    {
        uint8_t *ptr = nullptr;
        uint32_t len = 0;
    };

    explicit FrameParser(uint32_t max_data_size = 4 << 20);

    // Set the pool for data buffer allocation (called by Connection).
    void set_pool(BufferPool *p)
    {
        pool_ = p;
    }

    ParseState state() const
    {
        return state_;
    }

    // Where to read next. len == 0 means no more bytes needed right now.
    ReadTarget next_read_target();

    // Mark n bytes as consumed. Transitions state, allocates the next
    // buffer if needed. Returns a complete Frame* when the frame is done,
    // or nullptr if more bytes are needed. On error, sets error_ and
    // returns nullptr (caller should check last_error()).
    Frame *advance(uint32_t bytes_read);

    // Feed bytes from an external buffer (e.g. a per-worker receive
    // buffer filled by one big read()). Processes header + control,
    // yields complete control-only frames via the callback. Stops when
    // the parser enters ReadingData state — the read loop handles data
    // directly (separate read for data into pool Buffer).
    // Returns the number of bytes consumed.
    template <typename Callback> uint32_t feed_data(const uint8_t *data, uint32_t len, Callback &&on_frame)
    {
        uint32_t consumed = 0;
        while (consumed < len && error_ == FramingError::None) {
            // Stop at data — the read loop handles data directly.
            if (state_ == ParseState::ReadingData) {
                break;
            }
            auto target = next_read_target();
            if (target.len == 0) {
                break;
            }
            uint32_t to_copy = target.len;
            if (to_copy > len - consumed) {
                to_copy = len - consumed;
            }
            std::memcpy(target.ptr, data + consumed, to_copy);
            consumed += to_copy;
            Frame *frame = advance(to_copy);
            if (frame) {
                on_frame(frame);
            }
        }
        return consumed;
    }

    // Reset to ReadingHeader (after a frame is yielded or on error).
    void reset();

    FramingError last_error() const
    {
        return error_;
    }

  private:
    uint32_t     max_data_size_;
    ParseState   state_ = ParseState::ReadingHeader;
    FramingError error_ = FramingError::None;

    Header   header_;
    uint8_t  header_buf_[HEADER_SIZE];
    uint32_t header_offset_ = 0;

    // Control: reused across frames (resized to msg_size).
    std::vector<uint8_t> control_buf_;
    uint32_t             control_offset_ = 0;

    // Data: pool-allocated Buffer (replaces malloc'd data_).
    Buffer  *data_buf_    = nullptr;
    uint32_t data_offset_ = 0;

    // Pool for data allocation.
    BufferPool *pool_ = nullptr;

    // Extracted during parse (stored in Frame on yield).
    uint64_t parsed_request_id_           = 0;
    uint64_t parsed_rpc_create_nano_      = 0;
    uint64_t parsed_response_create_nano_ = 0;

    // The completed frame (returned by advance, owned by the caller).
    Frame *frame_ = nullptr;

    // Validate the parsed header; returns FramingError::None if ok.
    FramingError validate_header() const;

    // Build and return the completed Frame; reset state for next frame.
    Frame *yield_frame();
};

} // namespace crow::rpc
