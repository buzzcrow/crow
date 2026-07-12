# Vendored LZ4 (PT13)

crowtree's page compression (PT10) uses the LZ4 block API
(`LZ4_compress_default` / `LZ4_decompress_safe`). For a portable build with **no
system dependency**, drop the official single-file LZ4 sources here:

```
crowtree/third_party/lz4/
  lz4.c          # from https://github.com/lz4/lz4 (lib/lz4.c)
  lz4.h          # from https://github.com/lz4/lz4 (lib/lz4.h)
  LICENSE        # LZ4's BSD-2-Clause license text
```

LZ4 is BSD-2-Clause licensed; keep its `LICENSE` alongside the sources.

## Build resolution

`CMakeLists.txt` resolves LZ4 in this order:

1. **Vendored source** — if both `lz4.c` and `lz4.h` exist in this directory,
   they are compiled straight into `libcrowtree` (no system dep). This is the
   preferred, portable path.
2. **System dev package** — otherwise CMake discovers `lz4.h` + `liblz4` via
   `find_path` / `find_library` (e.g. a distro `-dev` package or the pixi env).
3. **None** — if neither is found, compression compiles out to an identity codec
   (`Options.compression = kLz4` still works, storing pages raw). The engine
   stays correct; only the on-disk size benefit is lost.

No distro-specific runtime soname is hard-coded anymore (the old
`/usr/lib/.../liblz4.so.1`-by-path link was removed).

## Portability test (PT13.4)

To validate the fully-vendored, no-system-LZ4 path:

```bash
# With lz4.c / lz4.h present in this directory and no system LZ4 visible:
cmake -S crowtree -B build-vendor
cmake --build build-vendor -j
ctest --test-dir build-vendor -R 'Compression|PageCompression'
```

The page-compression unit + integration tests (`unit/page_compression_test.cc`,
`integration/compression_test.cc`) must pass against the vendored codec.
