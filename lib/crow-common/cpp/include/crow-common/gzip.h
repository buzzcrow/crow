// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <string>

namespace crow::common
{

// Compress a file with gzip (zlib). Reads `src_path`, writes
// `src_path + ".gz"`, then deletes `src_path`. Returns true on success.
// On failure, leaves the original file intact and returns false.
bool gzip_compress_file(const std::string &src_path);

} // namespace crow::common
