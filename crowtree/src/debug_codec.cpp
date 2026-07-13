// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crowtree/debug_codec.h"

#include "crowtree/cell.h"
#include "crowtree/frame_page.h"

#include <array>
#include <cctype>
#include <sstream>

namespace crowtree
{

namespace
{

const std::array<char, 17> kHex = {"0123456789abcdef"};

void append_hex(std::string *out, const uint8_t *p, size_t n)
{
    for (size_t i = 0; i < n; ++i) {
        out->push_back(kHex[p[i] >> 4]);
        out->push_back(kHex[p[i] & 0xf]);
    }
}

void append_hex(std::string *out, Slice s)
{
    append_hex(out, reinterpret_cast<const uint8_t *>(s.data()), s.size());
}

int hex_val(char c)
{
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

bool decode_hex(const std::string &s, std::vector<uint8_t> *out)
{
    if (s.size() % 2 != 0) {
        return false;
    }
    out->clear();
    out->reserve(s.size() / 2);
    for (size_t i = 0; i < s.size(); i += 2) {
        int hi = hex_val(s[i]);
        int lo = hex_val(s[i + 1]);
        if (hi < 0 || lo < 0) {
            return false;
        }
        out->push_back(static_cast<uint8_t>((hi << 4) | lo));
    }
    return true;
}

const char *type_name(page_type t)
{
    switch (t) {
    case page_type::kLeafBase:
        return "leaf";
    case page_type::kInnerBase:
        return "inner";
    case page_type::kOverflowFrame:
        return "overflow";
    default:
        return "unknown";
    }
}

} // namespace

std::string encode_frame_text(const uint8_t *frame, uint32_t plen)
{
    std::string out;
    out += "crowtree-frame-text 1\n";
    out += "plen " + std::to_string(plen) + "\n";
    page_type t = frame_page_type(frame);
    out += std::string("type ") + type_name(t) + "\n";

    // Informational, human-readable per-slot annotations (decode ignores these and
    // reconstructs from the `raw` line, so they stay safe even if a frame is odd).
    if (plen > kFrameHeaderSize + kFrameTrailerSize && frame_validate(frame, plen)) {
        if (t == page_type::kLeafBase) {
            LeafFrameView v(frame, plen);
            out += "self_page_id " + std::to_string(v.self_page_id()) + " right_sibling " +
                   std::to_string(v.right_sibling()) + " count " + std::to_string(v.count()) + "\n";
            for (uint32_t i = 0; i < v.count(); ++i) {
                CellView c{v.cell(i)};
                out += "  kv key=";
                append_hex(&out, v.key(i));
                out += " slot=" + std::to_string(c.slot());
                out += c.is_tombstone()
                         ? " kind=del"
                         : (c.is_overflow() ? " kind=overflow"
                                            : " kind=put"); // NOLINT(readability-avoid-nested-conditional-operator)
                out += " cell=";
                append_hex(&out, c.raw());
                out += "\n";
            }
        }
        else if (t == page_type::kInnerBase) {
            InnerFrameView v(frame, plen);
            out += "self_page_id " + std::to_string(v.self_page_id()) + " separators " +
                   std::to_string(v.num_separators()) + "\n";
            for (uint32_t i = 0; i < v.num_children(); ++i) {
                out += "  child " + std::to_string(v.child_at(i)) + "\n";
            }
            for (uint32_t i = 0; i < v.num_separators(); ++i) {
                out += "  sep ";
                append_hex(&out, v.separator_at(i));
                out += "\n";
            }
        }
        else if (t == page_type::kOverflowFrame) {
            OverflowFrameView v(frame, plen);
            out += "self_page_id " + std::to_string(v.self_page_id()) + " next_page_id " +
                   std::to_string(v.next_page_id()) + " chunk_len " + std::to_string(v.chunk_len()) + "\n";
        }
    }

    out += "raw ";
    append_hex(&out, frame, plen);
    out += "\n";
    return out;
}

Status decode_frame_text(const std::string &text, std::vector<uint8_t> *out)
{
    std::istringstream   in(text);
    std::string          line;
    bool                 have_header = false;
    uint64_t             plen        = 0;
    bool                 have_plen   = false;
    std::vector<uint8_t> raw;
    bool                 have_raw = false;
    while (std::getline(in, line)) {
        // Trim leading spaces (annotation lines are indented).
        size_t start = line.find_first_not_of(" \t");
        if (start == std::string::npos) {
            continue;
        }
        std::string body = line.substr(start);
        if (body.starts_with("crowtree-frame-text")) {
            have_header = true;
        }
        else if (body.starts_with("plen ")) {
            plen      = std::strtoull(body.c_str() + 5, nullptr, 10);
            have_plen = true;
        }
        else if (body.starts_with("raw ")) {
            if (!decode_hex(body.substr(4), &raw)) {
                return Status::corruption("debug_codec: bad raw hex");
            }
            have_raw = true;
        }
    }
    if (!have_header) {
        return Status::invalid_argument("debug_codec: missing header");
    }
    if (!have_plen || !have_raw) {
        return Status::invalid_argument("debug_codec: missing plen/raw");
    }
    if (raw.size() != plen) {
        return Status::corruption("debug_codec: raw length != plen");
    }
    *out = std::move(raw);
    return Status::Ok();
}

} // namespace crowtree
