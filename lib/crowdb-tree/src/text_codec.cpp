// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/text_codec.h"

#include <cstdio>
#include <cstring>
#include <sstream>

namespace crowdb::tree
{

namespace
{

// Hex-encode a byte range.
std::string to_hex(const uint8_t *buf, size_t len)
{
    std::string out;
    out.reserve(len * 2);
    static const char hex[] = "0123456789abcdef";
    for (size_t i = 0; i < len; ++i) {
        out.push_back(hex[buf[i] >> 4]);
        out.push_back(hex[buf[i] & 0xf]);
    }
    return out;
}

// Hex-decode a string into bytes. Returns false on invalid input.
bool from_hex(const std::string &hex, std::vector<uint8_t> *out)
{
    if (hex.size() % 2 != 0) {
        return false;
    }
    out->clear();
    out->reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2) {
        auto hi  = hex[i];
        auto lo  = hex[i + 1];
        auto val = [](char c) -> int {
            if (c >= '0' && c <= '9')
                return c - '0';
            if (c >= 'a' && c <= 'f')
                return c - 'a' + 10;
            if (c >= 'A' && c <= 'F')
                return c - 'A' + 10;
            return -1;
        };
        int h = val(hi);
        int l = val(lo);
        if (h < 0 || l < 0) {
            return false;
        }
        out->push_back(static_cast<uint8_t>((h << 4) | l));
    }
    return true;
}

// Read a little-endian u32 from a byte buffer.
uint32_t get_u32(const uint8_t *p)
{
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) | (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}

// Read a little-endian u64 from a byte buffer.
uint64_t get_u64(const uint8_t *p)
{
    return static_cast<uint64_t>(p[0]) | (static_cast<uint64_t>(p[1]) << 8) | (static_cast<uint64_t>(p[2]) << 16) |
           (static_cast<uint64_t>(p[3]) << 24) | (static_cast<uint64_t>(p[4]) << 32) |
           (static_cast<uint64_t>(p[5]) << 40) | (static_cast<uint64_t>(p[6]) << 48) |
           (static_cast<uint64_t>(p[7]) << 56);
}

// Generic text codec: produce a header line with magic + type name, then a
// raw hex line for exact round-trip. The header is human-readable but the
// raw line is what decode uses for the exact bytes.
std::string encode_generic_text(const char *type_name, uint32_t magic, const uint8_t *buf, size_t len)
{
    std::ostringstream oss;
    oss << "CROWDB_CT_" << type_name << " magic=0x" << to_hex(reinterpret_cast<const uint8_t *>(&magic), 4);
    oss << " len=" << len << "\n";
    oss << "raw " << to_hex(buf, len) << "\n";
    return oss.str();
}

// Generic decode: find the "raw " line and hex-decode it.
Status decode_generic_text(const std::string &text, std::vector<uint8_t> *out)
{
    std::istringstream iss(text);
    std::string        line;
    while (std::getline(iss, line)) {
        if (line.substr(0, 4) == "raw ") {
            if (!from_hex(line.substr(4), out)) {
                return Status::corruption("text codec: invalid hex in raw line");
            }
            return Status::Ok();
        }
    }
    return Status::corruption("text codec: no raw line found");
}

} // namespace

// ── Anchor ────────────────────────────────────────────────────────

std::string encode_anchor_text(const uint8_t *buf, size_t len)
{
    if (len < 60) {
        return encode_generic_text("ANCHOR", 0, buf, len);
    }
    uint32_t magic          = get_u32(buf);
    uint32_t format_version = get_u32(buf + 4);
    uint64_t snapshot_seq   = get_u64(buf + 8);
    uint64_t root_page_id   = get_u64(buf + 16);
    uint64_t last_applied   = get_u64(buf + 24);
    uint64_t next_page_id   = get_u64(buf + 32);
    uint32_t segment_slots  = get_u32(buf + 40);
    uint64_t segdir_addr    = get_u64(buf + 44);
    uint32_t segdir_len     = get_u32(buf + 52);
    uint32_t segdir_crc     = get_u32(buf + 56);

    std::ostringstream oss;
    oss << "CROWDB_CT_ANCHOR magic=0x" << to_hex(reinterpret_cast<const uint8_t *>(&magic), 4);
    oss << " format_version=" << format_version;
    oss << " snapshot_seq=" << snapshot_seq;
    oss << " root_page_id=" << root_page_id;
    oss << " last_applied_slot=" << last_applied;
    oss << " next_page_id=" << next_page_id;
    oss << " segment_slots=" << segment_slots;
    oss << " segdir_addr=" << segdir_addr;
    oss << " segdir_len=" << segdir_len;
    oss << " segdir_crc=" << std::hex << segdir_crc << std::dec;
    oss << " len=" << len << "\n";
    oss << "raw " << to_hex(buf, len) << "\n";
    return oss.str();
}

Status decode_anchor_text(const std::string &text, std::vector<uint8_t> *out)
{
    return decode_generic_text(text, out);
}

// ── Segment image ─────────────────────────────────────────────────

std::string encode_seg_image_text(const uint8_t *buf, size_t len)
{
    if (len < 24) {
        return encode_generic_text("SEGIMG", 0, buf, len);
    }
    uint32_t magic      = get_u32(buf);
    uint32_t seg_idx    = get_u32(buf + 4);
    uint64_t generation = get_u64(buf + 8);
    uint32_t slot_count = get_u32(buf + 16);
    uint32_t live_count = get_u32(buf + 20);

    std::ostringstream oss;
    oss << "CROWDB_CT_SEGIMG magic=0x" << to_hex(reinterpret_cast<const uint8_t *>(&magic), 4);
    oss << " seg_idx=" << seg_idx;
    oss << " generation=" << generation;
    oss << " slot_count=" << slot_count;
    oss << " live_count=" << live_count;
    oss << " len=" << len << "\n";
    oss << "raw " << to_hex(buf, len) << "\n";
    return oss.str();
}

Status decode_seg_image_text(const std::string &text, std::vector<uint8_t> *out)
{
    return decode_generic_text(text, out);
}

// ── Segment directory ─────────────────────────────────────────────

std::string encode_segdir_text(const uint8_t *buf, size_t len)
{
    if (len < 12) {
        return encode_generic_text("SEGDIR", 0, buf, len);
    }
    uint32_t magic = get_u32(buf);
    uint32_t count = get_u32(buf + 4);

    std::ostringstream oss;
    oss << "CROWDB_CT_SEGDIR magic=0x" << to_hex(reinterpret_cast<const uint8_t *>(&magic), 4);
    oss << " count=" << count;
    oss << " len=" << len << "\n";
    oss << "raw " << to_hex(buf, len) << "\n";
    return oss.str();
}

Status decode_segdir_text(const std::string &text, std::vector<uint8_t> *out)
{
    return decode_generic_text(text, out);
}

} // namespace crowdb::tree
