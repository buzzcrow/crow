// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>
#include <cstdlib>
#include <cstring>

namespace crow::rpc
{

// ── Wire format (12-byte header) ──────────────────────────────────
//
// [magic:2][msg_type:2][msg_size:2][data_size:4][msg_offset:1][flags:1]
//
// Little-endian, field-by-field (not memcpy of the struct — avoids
// compiler-layout dependence). See design-crow-rpc.md §3 for
// the rationale behind each field and what was removed from the
// reference's 20-byte header.

constexpr uint16_t MAGIC       = 0xCA70;
constexpr uint8_t  HEADER_SIZE = 12;

// flags bit definitions
constexpr uint8_t FLAG_ONE_WAY = 0x01;

struct Header
{
    uint16_t magic      = MAGIC;
    uint16_t msg_type   = 0;
    uint16_t msg_size   = 0;           // control message length
    uint32_t data_size  = 0;           // data payload length
    uint8_t  msg_offset = HEADER_SIZE; // offset to control message
    uint8_t  flags      = 0;
};

// A complete frame: header + control buffer + optional data buffer.
// Ownership: control and data are malloc'd by the parser and freed by
// the destructor. The handler/callback that receives a Frame* must
// delete it when done (or transfer ownership by nulling the pointers).
struct Frame
{
    Header   header;
    uint8_t *control     = nullptr; // flatbuffer control message bytes
    uint32_t control_len = 0;
    uint8_t *data        = nullptr; // raw data payload, nullptr if control-only
    uint32_t data_len    = 0;

    ~Frame()
    {
        std::free(control);
        std::free(data);
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

// ── FrameParser — pull-based zero-copy state machine ──────────────
//
// The parser drives receive-side zero-copy. Instead of reading into a
// scratch buffer and copying, it tells the read loop *where to read
// next* — directly into pool-allocated Buffers. This unifies the TCP and
// RDMA receive paths.
//
// Usage in the read loop:
//   target = parser.next_read_target();
//   n = read(fd, target.ptr, target.len);
//   frame = parser.advance(n);
//   if (frame) dispatch(frame);

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

    // Where to read next. len == 0 means no more bytes needed right now.
    ReadTarget next_read_target();

    // Mark n bytes as consumed. Transitions state, allocates the next
    // buffer if needed. Returns a complete Frame* when the frame is done,
    // or nullptr if more bytes are needed. On error, sets error_ and
    // returns nullptr (caller should check last_error()).
    Frame *advance(uint32_t bytes_read);

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

    // Control message (allocated by the caller via the pool, or by the
    // parser itself for testing). The parser owns these allocations.
    uint8_t *control_        = nullptr;
    uint32_t control_len_    = 0;
    uint32_t control_offset_ = 0;

    // Data payload.
    uint8_t *data_        = nullptr;
    uint32_t data_len_    = 0;
    uint32_t data_offset_ = 0;

    // The completed frame (returned by advance, owned by the caller).
    Frame *frame_ = nullptr;

    // Allocate control/data buffers. Override in tests; production uses
    // BufferPool. Default: malloc (freed on reset).
    virtual uint8_t *alloc_buf(uint32_t capacity);
    virtual void     free_buf(uint8_t *ptr);

    // Validate the parsed header; returns FramingError::None if ok.
    FramingError validate_header() const;

    // Build and return the completed Frame; reset state for next frame.
    Frame *yield_frame();
};

} // namespace crow::rpc
