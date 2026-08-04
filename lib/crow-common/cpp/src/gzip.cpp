// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#include "crow-common/gzip.h"

#include <zlib.h>

#include <array>
#include <cstdio>

namespace crow::common
{

bool gzip_compress_file(const std::string &src_path)
{
    std::FILE *src = std::fopen(src_path.c_str(), "rb");
    if (src == nullptr) {
        return false;
    }

    const std::string dst_path = src_path + ".gz";
    gzFile            dst      = gzopen(dst_path.c_str(), "wb");
    if (dst == nullptr) {
        std::fclose(src);
        return false;
    }

    constexpr int                       kBufSize = 64 * 1024;
    std::array<unsigned char, kBufSize> buf;
    bool                                ok = true;
    while (true) {
        std::size_t n = std::fread(buf.data(), 1, kBufSize, src); // NOLINT(clang-analyzer-unix.Stream)
        if (n == 0) {
            if (std::ferror(src) != 0) {
                ok = false;
            }
            break;
        }
        if (gzwrite(dst, buf.data(), static_cast<unsigned>(n)) != static_cast<int>(n)) {
            ok = false;
            break;
        }
    }

    std::fclose(src);
    if (gzclose(dst) != Z_OK) {
        ok = false;
    }

    if (ok) {
        std::remove(src_path.c_str());
    }
    else {
        std::remove(dst_path.c_str());
    }
    return ok;
}

} // namespace crow::common
