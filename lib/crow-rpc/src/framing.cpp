// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-rpc/framing.h"

#include "crow-rpc/buffer.h"

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <new>

namespace crow::rpc
{

// extract_control_fields is implemented in message.cpp (needs flatbuffer
// generated headers not available here).

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
    h.magic      = static_cast<uint16_t>(buf[0]) | (static_cast<uint16_t>(buf[1]) << 8);
    h.msg_type   = static_cast<uint16_t>(buf[2]) | (static_cast<uint16_t>(buf[3]) << 8);
    h.msg_size   = static_cast<uint16_t>(buf[4]) | (static_cast<uint16_t>(buf[5]) << 8);
    h.data_size  = static_cast<uint32_t>(buf[6]) | (static_cast<uint32_t>(buf[7]) << 8) |
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
    control_buf_.clear();
    control_offset_ = 0;
    if (data_buf_ != nullptr) {
        data_buf_->release();
        data_buf_ = nullptr;
    }
    data_offset_ = 0;
    if (frame_ != nullptr) {
        delete frame_;
        frame_ = nullptr;
    }
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
    frame_->header          = header_;
    frame_->request_id      = parsed_request_id_;
    frame_->rpc_create_nano = parsed_rpc_create_nano_;
    frame_->data_buf        = data_buf_;
    frame_->parsed_nano     = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());

    // Transfer ownership to the caller — clear our pointers so reset()
    // doesn't release them.
    Frame *out = frame_;
    frame_     = nullptr;
    data_buf_  = nullptr;

    // Reset for the next frame.
    state_         = ParseState::ReadingHeader;
    header_offset_ = 0;
    control_buf_.clear();
    control_offset_         = 0;
    data_offset_            = 0;
    parsed_request_id_      = 0;
    parsed_rpc_create_nano_ = 0;

    return out;
}

FrameParser::ReadTarget FrameParser::next_read_target()
{
    switch (state_) {
    case ParseState::ReadingHeader:
        return {header_buf_ + header_offset_, HEADER_SIZE - header_offset_};
    case ParseState::ReadingControl:
        return {control_buf_.data() + control_offset_, static_cast<uint32_t>(control_buf_.size()) - control_offset_};
    case ParseState::ReadingData:
        if (data_buf_ != nullptr) {
            return {data_buf_->data + data_offset_, data_buf_->capacity - data_offset_};
        }
        return {nullptr, 0};
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
        if (header_.msg_size == 0) {
            // No control message.
            parsed_request_id_      = 0;
            parsed_rpc_create_nano_ = 0;
            if (header_.data_size == 0) {
                // Control-only, data-less frame (e.g. one-way ping).
                frame_ = new Frame;
                return yield_frame();
            }
            // Data-only frame (no control message).
            if (pool_ != nullptr) {
                data_buf_ = pool_->alloc(header_.data_size);
            }
            if (data_buf_ == nullptr) {
                error_ = FramingError::DataTooLarge;
                return nullptr;
            }
            state_ = ParseState::ReadingData;
        }
        else {
            // Allocate control buffer (reused vector).
            control_buf_.resize(header_.msg_size);
            control_offset_ = 0;
            state_          = ParseState::ReadingControl;
        }
        return nullptr;
    }

    case ParseState::ReadingControl: {
        control_offset_ += bytes_read;
        if (control_offset_ < control_buf_.size()) {
            return nullptr; // need more control bytes
        }
        // Control complete — extract fields (buzz-cpp style).
        extract_control_fields(control_buf_.data(), static_cast<uint32_t>(control_buf_.size()), parsed_request_id_,
                               parsed_rpc_create_nano_);
        if (header_.data_size == 0) {
            // Control-only frame.
            frame_ = new Frame;
            return yield_frame();
        }
        // Allocate data buffer from pool.
        if (pool_ != nullptr) {
            data_buf_ = pool_->alloc(header_.data_size);
        }
        if (data_buf_ == nullptr) {
            error_ = FramingError::DataTooLarge;
            return nullptr;
        }
        state_ = ParseState::ReadingData;
        return nullptr;
    }

    case ParseState::ReadingData: {
        data_offset_ += bytes_read;
        if (data_buf_ == nullptr) {
            return nullptr;
        }
        if (data_offset_ < data_buf_->capacity) {
            return nullptr; // need more data bytes
        }
        // Frame complete — set len so consumers know how much data is valid.
        data_buf_->len = data_offset_;
        frame_         = new Frame;
        return yield_frame();
    }
    }
    return nullptr;
}

} // namespace crow::rpc
