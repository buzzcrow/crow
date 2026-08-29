// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/mapping_persist.h"

#include "crowdb-common/crc32c.h"

namespace crowdb::tree
{

namespace
{

constexpr uint32_t kSegImageMagic = 0x534D5443; // 'CTMS' little-endian byte order
constexpr uint32_t kSegDirMagic   = 0x44535443; // 'CTSD' little-endian byte order
constexpr uint16_t kFormatVersion = 1;

// Fixed header sizes (bytes), before the variable-length body.
constexpr size_t kImageHeaderBytes = 4 + 2 + 2 + 4 + 8 + 4 + 4 + 4; // magic..live_count,header_crc
constexpr size_t kDirHeaderBytes   = 4 + 2 + 2 + 4 + 4;             // magic,version,flags,entry_count,header_crc
constexpr size_t kDirEntryBytes    = 4 + 4 + 8 + 8 + 4 + 4;         // seg_idx,pad,generation,addr,len,crc

void put_u16(std::vector<uint8_t> *out, uint16_t v)
{
    out->push_back(static_cast<uint8_t>(v & 0xff));
    out->push_back(static_cast<uint8_t>((v >> 8) & 0xff));
}

void put_u32(std::vector<uint8_t> *out, uint32_t v)
{
    for (int i = 0; i < 4; ++i) {
        out->push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
    }
}

void put_u64(std::vector<uint8_t> *out, uint64_t v)
{
    for (int i = 0; i < 8; ++i) {
        out->push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
    }
}

uint16_t get_u16(const uint8_t *p)
{
    return static_cast<uint16_t>(p[0]) | (static_cast<uint16_t>(p[1]) << 8);
}

uint32_t get_u32(const uint8_t *p)
{
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v |= static_cast<uint32_t>(p[i]) << (8 * i);
    }
    return v;
}

uint64_t get_u64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(p[i]) << (8 * i);
    }
    return v;
}

} // namespace

size_t segment_image_encoded_size(uint32_t slot_count)
{
    return kImageHeaderBytes + (static_cast<size_t>(slot_count) * 8) + 4 /* body_crc trailer */;
}

void encode_segment_image(const SegmentImageHeader &hdr, const std::vector<uint64_t> &words, std::vector<uint8_t> *out,
                          uint32_t *out_body_crc)
{
    size_t base = out->size();
    put_u32(out, kSegImageMagic);
    put_u16(out, kFormatVersion);
    put_u16(out, 0); // flags, reserved
    put_u32(out, hdr.seg_idx);
    put_u64(out, hdr.generation);
    put_u32(out, hdr.slot_count);
    put_u32(out, hdr.live_count);
    uint32_t header_crc = crowdb::common::crc32c(out->data() + base, out->size() - base);
    put_u32(out, header_crc);

    size_t body_base = out->size();
    for (uint64_t w : words) {
        put_u64(out, w);
    }
    uint32_t body_crc = crowdb::common::crc32c(out->data() + body_base, out->size() - body_base);
    put_u32(out, body_crc);
    if (out_body_crc != nullptr) {
        *out_body_crc = body_crc;
    }
}

Status decode_segment_image(const uint8_t *buf, size_t len, SegmentImageHeader *hdr_out,
                            std::vector<uint64_t> *words_out)
{
    if (len < kImageHeaderBytes) {
        return Status::corruption("segment image: short header");
    }
    if (get_u32(buf) != kSegImageMagic) {
        return Status::corruption("segment image: bad magic");
    }
    uint32_t stored_header_crc = get_u32(buf + (kImageHeaderBytes - 4));
    if (crowdb::common::crc32c(buf, kImageHeaderBytes - 4) != stored_header_crc) {
        return Status::corruption("segment image: header CRC mismatch");
    }
    // Clean-break format: no older format to
    // accept, so a version mismatch is just corruption/foreign-format, same
    // as a bad magic.
    if (get_u16(buf + 4) != kFormatVersion) {
        return Status::corruption("segment image: unsupported format_version");
    }
    uint32_t seg_idx    = get_u32(buf + 8);
    uint64_t generation = get_u64(buf + 12);
    uint32_t slot_count = get_u32(buf + 20);
    uint32_t live_count = get_u32(buf + 24);

    size_t body_bytes = static_cast<size_t>(slot_count) * 8;
    if (len < kImageHeaderBytes + body_bytes + 4) {
        return Status::corruption("segment image: short body");
    }
    const uint8_t *body     = buf + kImageHeaderBytes;
    uint32_t       body_crc = get_u32(body + body_bytes);
    if (crowdb::common::crc32c(body, body_bytes) != body_crc) {
        return Status::corruption("segment image: body CRC mismatch");
    }

    hdr_out->seg_idx    = seg_idx;
    hdr_out->generation = generation;
    hdr_out->body_crc   = body_crc;
    hdr_out->slot_count = slot_count;
    hdr_out->live_count = live_count;
    words_out->clear();
    words_out->reserve(slot_count);
    for (uint32_t i = 0; i < slot_count; ++i) {
        words_out->push_back(get_u64(body + (static_cast<size_t>(i) * 8)));
    }
    return Status::Ok();
}

void encode_segment_directory(const std::vector<DirEntry> &entries, std::vector<uint8_t> *out)
{
    size_t base = out->size();
    put_u32(out, kSegDirMagic);
    put_u16(out, kFormatVersion);
    put_u16(out, 0); // flags, reserved
    put_u32(out, static_cast<uint32_t>(entries.size()));
    uint32_t header_crc = crowdb::common::crc32c(out->data() + base, out->size() - base);
    put_u32(out, header_crc);

    size_t body_base = out->size();
    for (const DirEntry &e : entries) {
        put_u32(out, e.seg_idx);
        put_u32(out, 0); // pad, reserved
        put_u64(out, e.generation);
        put_u64(out, e.image_addr);
        put_u32(out, e.image_len);
        put_u32(out, e.image_crc);
    }
    uint32_t body_crc = crowdb::common::crc32c(out->data() + body_base, out->size() - body_base);
    put_u32(out, body_crc);
}

Status decode_segment_directory(const uint8_t *buf, size_t len, std::vector<DirEntry> *entries_out)
{
    if (len < kDirHeaderBytes) {
        return Status::corruption("segment directory: short header");
    }
    if (get_u32(buf) != kSegDirMagic) {
        return Status::corruption("segment directory: bad magic");
    }
    uint32_t stored_header_crc = get_u32(buf + (kDirHeaderBytes - 4));
    if (crowdb::common::crc32c(buf, kDirHeaderBytes - 4) != stored_header_crc) {
        return Status::corruption("segment directory: header CRC mismatch");
    }
    if (get_u16(buf + 4) != kFormatVersion) {
        return Status::corruption("segment directory: unsupported format_version");
    }
    uint32_t entry_count = get_u32(buf + 8);

    size_t body_bytes = static_cast<size_t>(entry_count) * kDirEntryBytes;
    if (len < kDirHeaderBytes + body_bytes + 4) {
        return Status::corruption("segment directory: short body");
    }
    const uint8_t *body     = buf + kDirHeaderBytes;
    uint32_t       body_crc = get_u32(body + body_bytes);
    if (crowdb::common::crc32c(body, body_bytes) != body_crc) {
        return Status::corruption("segment directory: body CRC mismatch");
    }

    entries_out->clear();
    entries_out->reserve(entry_count);
    for (uint32_t i = 0; i < entry_count; ++i) {
        const uint8_t *e = body + (static_cast<size_t>(i) * kDirEntryBytes);
        DirEntry       d;
        d.seg_idx    = get_u32(e);
        d.generation = get_u64(e + 8);
        d.image_addr = get_u64(e + 16);
        d.image_len  = get_u32(e + 24);
        d.image_crc  = get_u32(e + 28);
        entries_out->push_back(d);
    }
    return Status::Ok();
}

} // namespace crowdb::tree
