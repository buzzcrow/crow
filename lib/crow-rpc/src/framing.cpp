// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/framing.h"

#include <cassert>
#include <cstdlib>
#include <new>

namespace crow::rpc
{

// ── Header serialize/parse (little-endian, field-by-field) ────────

void serialize_header(uint8_t *buf, const Header &h)
{
    buf[0]  = static_cast<uint8_t>(h.magic & 0xFF);
    buf[1]  = static_cast<uint8_t>(h.magic >> 8);
    buf[2]  = static_cast<uint8_t>(h.msg_type & 0xFF);
    buf[3]  = static_cast<uint8_t>(h.msg_type >> 8);
    buf[4]  = static_cast<uint8_t>(h.msg_size & 0xFF);
    buf[5]  = static_cast<uint8_t>(h.msg_size >> 8);
    buf[6]  = static_cast<uint8_t>(h.data_size & 0xFF);
    buf[7]  = static_cast<uint8_t>(h.data_size >> 8);
    buf[8]  = static_cast<uint8_t>(h.data_size >> 16);
    buf[9]  = static_cast<uint8_t>(h.data_size >> 24);
    buf[10] = h.msg_offset;
    buf[11] = h.flags;
}

Header parse_header(const uint8_t *buf)
{
    Header h;
    h.magic     = static_cast<uint16_t>(buf[0]) | (static_cast<uint16_t>(buf[1]) << 8);
    h.msg_type  = static_cast<uint16_t>(buf[2]) | (static_cast<uint16_t>(buf[3]) << 8);
    h.msg_size  = static_cast<uint16_t>(buf[4]) | (static_cast<uint16_t>(buf[5]) << 8);
    h.data_size = static_cast<uint32_t>(buf[6]) | (static_cast<uint32_t>(buf[7]) << 8) |
                  (static_cast<uint32_t>(buf[8]) << 16) | (static_cast<uint32_t>(buf[9]) << 24);
    h.msg_offset = buf[10];
    h.flags      = buf[11];
    return h;
}

// ── FrameParser ───────────────────────────────────────────────────

FrameParser::FrameParser(uint32_t max_data_size) : max_data_size_(max_data_size)
{
    reset();
}

void FrameParser::reset()
{
    state_         = ParseState::ReadingHeader;
    error_         = FramingError::None;
    header_offset_ = 0;
    if (control_ != nullptr) {
        free_buf(control_);
        control_ = nullptr;
    }
    control_len_    = 0;
    control_offset_ = 0;
    if (data_ != nullptr) {
        free_buf(data_);
        data_ = nullptr;
    }
    data_len_    = 0;
    data_offset_ = 0;
    if (frame_ != nullptr) {
        delete frame_;
        frame_ = nullptr;
    }
}

uint8_t *FrameParser::alloc_buf(uint32_t capacity)
{
    return static_cast<uint8_t *>(std::malloc(capacity));
}

void FrameParser::free_buf(uint8_t *ptr)
{
    std::free(ptr);
}

FramingError FrameParser::validate_header() const
{
    if (header_.magic != MAGIC) {
        return FramingError::BadMagic;
    }
    if (header_.msg_offset < HEADER_SIZE) {
        return FramingError::BadOffset;
    }
    if (header_.data_size > max_data_size_) {
        return FramingError::DataTooLarge;
    }
    return FramingError::None;
}

Frame *FrameParser::yield_frame()
{
    assert(frame_ != nullptr);
    frame_->header      = header_;
    frame_->control     = control_;
    frame_->control_len = control_len_;
    frame_->data        = data_;
    frame_->data_len    = data_len_;

    // Transfer ownership to the caller — clear our pointers so reset()
    // doesn't free them.
    Frame *out = frame_;
    frame_     = nullptr;
    control_   = nullptr;
    data_      = nullptr;

    // Reset for the next frame.
    state_          = ParseState::ReadingHeader;
    header_offset_  = 0;
    control_len_    = 0;
    control_offset_ = 0;
    data_len_       = 0;
    data_offset_    = 0;

    return out;
}

FrameParser::ReadTarget FrameParser::next_read_target()
{
    switch (state_) {
    case ParseState::ReadingHeader:
        return {header_buf_ + header_offset_, HEADER_SIZE - header_offset_};
    case ParseState::ReadingControl:
        return {control_ + control_offset_, control_len_ - control_offset_};
    case ParseState::ReadingData:
        return {data_ + data_offset_, data_len_ - data_offset_};
    }
    return {nullptr, 0};
}

Frame *FrameParser::advance(uint32_t bytes_read)
{
    if (error_ != FramingError::None) {
        return nullptr;
    }

    switch (state_) {
    case ParseState::ReadingHeader: {
        header_offset_ += bytes_read;
        if (header_offset_ < HEADER_SIZE) {
            return nullptr; // need more header bytes
        }
        // Header complete — parse and validate.
        header_ = parse_header(header_buf_);
        error_  = validate_header();
        if (error_ != FramingError::None) {
            return nullptr;
        }
        // Skip extra header bytes if msg_offset > HEADER_SIZE (forward
        // compat: a future header extension). For now msg_offset ==
        // HEADER_SIZE always.
        if (header_.msg_size == 0) {
            // No control message.
            if (header_.data_size == 0) {
                // Control-only, data-less frame (e.g. one-way ping).
                frame_ = new Frame;
                return yield_frame();
            }
            // Data-only frame (no control message).
            data_ = alloc_buf(header_.data_size);
            if (data_ == nullptr) {
                error_ = FramingError::DataTooLarge;
                return nullptr;
            }
            data_len_ = header_.data_size;
            state_    = ParseState::ReadingData;
        }
        else {
            // Allocate control buffer.
            control_ = alloc_buf(header_.msg_size);
            if (control_ == nullptr) {
                error_ = FramingError::DataTooLarge;
                return nullptr;
            }
            control_len_ = header_.msg_size;
            state_       = ParseState::ReadingControl;
        }
        return nullptr;
    }

    case ParseState::ReadingControl: {
        control_offset_ += bytes_read;
        if (control_offset_ < control_len_) {
            return nullptr; // need more control bytes
        }
        // Control complete.
        if (header_.data_size == 0) {
            // Control-only frame.
            frame_ = new Frame;
            return yield_frame();
        }
        // Allocate data buffer.
        data_ = alloc_buf(header_.data_size);
        if (data_ == nullptr) {
            error_ = FramingError::DataTooLarge;
            return nullptr;
        }
        data_len_ = header_.data_size;
        state_    = ParseState::ReadingData;
        return nullptr;
    }

    case ParseState::ReadingData: {
        data_offset_ += bytes_read;
        if (data_offset_ < data_len_) {
            return nullptr; // need more data bytes
        }
        // Frame complete.
        frame_ = new Frame;
        return yield_frame();
    }
    }
    return nullptr;
}

} // namespace crow::rpc
