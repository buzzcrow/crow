// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crowtree/text_page_store.h"

#include "crowtree/debug_codec.h"
#include "crowtree/text_codec.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>

namespace crowtree
{

namespace
{
// Anchor magic from persist.cpp (0x41435443 = 'CTCA' little-endian).
constexpr uint32_t kAnchorMagic = 0x41435443;
// Segment image magic from mapping_persist.cpp.
constexpr uint32_t kSegImageMagic = 0x534D5443; // 'CTMS' little-endian
// Segment directory magic from mapping_persist.cpp.
constexpr uint32_t kSegDirMagic = 0x44535443; // 'CTSD' little-endian

uint32_t get_u32(const uint8_t *p)
{
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

bool dir_exists(const std::string &path)
{
    struct stat st{};
    return ::stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

Status make_dir(const std::string &path)
{
    if (::mkdir(path.c_str(), 0755) < 0 && errno != EEXIST) {
        return Status::io_error(std::string("mkdir: ") + std::strerror(errno));
    }
    return Status::Ok();
}

Status write_file(const std::string &path, const std::string &content)
{
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    if (!ofs) {
        return Status::io_error("TextPageStore: cannot open " + path);
    }
    ofs.write(content.data(), static_cast<std::streamsize>(content.size()));
    if (!ofs) {
        return Status::io_error("TextPageStore: write failed " + path);
    }
    return Status::Ok();
}

Status read_file(const std::string &path, std::string *out)
{
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs) {
        return Status::io_error("TextPageStore: cannot open " + path);
    }
    std::ostringstream oss;
    oss << ifs.rdbuf();
    *out = oss.str();
    return Status::Ok();
}
} // namespace

TextPageStore::~TextPageStore() = default;

Status TextPageStore::open(const std::string &path, uint32_t store_id, uint32_t partition_id,
                           std::unique_ptr<TextPageStore> *out)
{
    std::string dir = path + "/" + std::to_string(store_id) + "-" + std::to_string(partition_id);
    if (!dir_exists(dir)) {
        Status s = make_dir(dir);
        if (!s.ok()) {
            return s;
        }
    }
    auto *store = new TextPageStore(dir);
    Status s    = store->load_manifest();
    if (!s.ok()) {
        delete store;
        return s;
    }
    out->reset(store);
    return Status::Ok();
}

Status TextPageStore::load_manifest()
{
    std::string manifest_path = dir_ + "/manifest.ck";
    std::string content;
    Status       s = read_file(manifest_path, &content);
    if (!s.ok()) {
        // No manifest = fresh store
        return Status::Ok();
    }
    std::istringstream iss(content);
    std::string        line;
    while (std::getline(iss, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }
        // Format: addr=N len=N file=filename
        ManifestEntry entry;
        std::istringstream liss(line);
        std::string        tok;
        while (liss >> tok) {
            if (tok.substr(0, 5) == "addr=") {
                entry.addr = std::stoull(tok.substr(5));
            } else if (tok.substr(0, 4) == "len=") {
                entry.len = std::stoull(tok.substr(4));
            } else if (tok.substr(0, 5) == "file=") {
                entry.filename = tok.substr(5);
            }
        }
        if (!entry.filename.empty()) {
            addr_index_[entry.addr] = entries_.size();
            entries_.push_back(std::move(entry));
        }
    }
    return Status::Ok();
}

Status TextPageStore::flush_manifest()
{
    if (!manifest_dirty_) {
        return Status::Ok();
    }
    std::ostringstream oss;
    oss << "# TextPageStore manifest\n";
    for (const auto &e : entries_) {
        oss << "addr=" << e.addr << " len=" << e.len << " file=" << e.filename << "\n";
    }
    Status s = write_file(dir_ + "/manifest.ck", oss.str());
    if (s.ok()) {
        manifest_dirty_ = false;
    }
    return s;
}

std::string TextPageStore::filename_for(uint64_t addr, const uint8_t *buf, size_t len) const
{
    if (len >= 4) {
        uint32_t magic = get_u32(buf);
        if (magic == kAnchorMagic) {
            // Anchor: slot 0 → anchor-A, slot 1 → anchor-B
            // The superblock slot size is 4096 (kAnchorBytes), so addr 0 → A,
            // addr 4096 → B.
            if (addr < 4096) {
                return "anchor-A.ck";
            }
            return "anchor-B.ck";
        }
        if (magic == kSegImageMagic) {
            return "seg-" + std::to_string(addr) + ".ck";
        }
        if (magic == kSegDirMagic) {
            return "segdir.ck";
        }
    }
    // Default: page blob
    return "page-" + std::to_string(addr) + ".ck";
}

std::string TextPageStore::encode_blob(const uint8_t *buf, size_t len) const
{
    if (len >= 4) {
        uint32_t magic = get_u32(buf);
        if (magic == kAnchorMagic) {
            return encode_anchor_text(buf, len);
        }
        if (magic == kSegImageMagic) {
            return encode_seg_image_text(buf, len);
        }
        if (magic == kSegDirMagic) {
            return encode_segdir_text(buf, len);
        }
    }
    // Default: page frame → debug_codec
    return encode_frame_text(buf, static_cast<uint32_t>(len));
}

Status TextPageStore::decode_file(const std::string &filename, std::vector<uint8_t> *out) const
{
    std::string content;
    Status       s = read_file(dir_ + "/" + filename, &content);
    if (!s.ok()) {
        return s;
    }

    // Determine type from filename prefix
    if (filename.substr(0, 6) == "anchor") {
        return decode_anchor_text(content, out);
    }
    if (filename.substr(0, 4) == "seg-") {
        return decode_seg_image_text(content, out);
    }
    if (filename.substr(0, 6) == "segdir") {
        return decode_segdir_text(content, out);
    }
    // Default: page frame
    return decode_frame_text(content, out);
}

Status TextPageStore::write_at(uint64_t off, const uint8_t *buf, size_t len)
{
    if (len == 0) {
        return Status::Ok();
    }

    std::string filename = filename_for(off, buf, len);
    std::string text     = encode_blob(buf, len);
    Status       s        = write_file(dir_ + "/" + filename, text);
    if (!s.ok()) {
        return s;
    }

    // Update or add manifest entry
    auto it = addr_index_.find(off);
    if (it != addr_index_.end()) {
        entries_[it->second].len      = len;
        entries_[it->second].filename = filename;
    } else {
        addr_index_[off] = entries_.size();
        entries_.push_back(ManifestEntry{.addr = off, .len = len, .filename = filename});
    }
    manifest_dirty_ = true;
    return Status::Ok();
}

Status TextPageStore::read_at(uint64_t off, uint8_t *buf, size_t len) const
{
    if (len == 0) {
        return Status::Ok();
    }

    auto it = addr_index_.find(off);
    if (it == addr_index_.end()) {
        return Status::io_error("TextPageStore: no file at addr " + std::to_string(off));
    }

    const auto &entry = entries_[it->second];
    std::vector<uint8_t> decoded;
    Status               s = decode_file(entry.filename, &decoded);
    if (!s.ok()) {
        return s;
    }
    if (decoded.size() < len) {
        return Status::io_error("TextPageStore: decoded blob shorter than requested read");
    }
    std::memcpy(buf, decoded.data(), len);
    return Status::Ok();
}

Status TextPageStore::sync()
{
    Status s = flush_manifest();
    if (!s.ok()) {
        return s;
    }
    // CT_SYNC_SKIP: no fsync (tests/CI only). Manifest is still flushed
    // to the OS page cache via ofstream above.
    if (sync_mode_ == SyncMode::kSkip) {
        return Status::Ok();
    }
    // fsync the directory to persist manifest changes
    int fd = ::open(dir_.c_str(), O_RDONLY, 0);
    if (fd >= 0) {
#if defined(__APPLE__)
        ::fsync(fd);
#else
        ::fdatasync(fd);
#endif
        ::close(fd);
    }
    return Status::Ok();
}

uint64_t TextPageStore::size() const
{
    uint64_t max_end = 0;
    for (const auto &e : entries_) {
        max_end = std::max(max_end, e.addr + e.len);
    }
    return max_end;
}

} // namespace crowtree
