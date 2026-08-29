// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Compile the crowdb-tree C++ engine (including the C ABI) into a static lib and
// link it into this crate. LZ4 is optional: set CROWDB_TREE_LZ4_LIB=/path/to/dir
// containing liblz4 to enable on-disk compression; otherwise the codec degrades
// to identity (stored raw) and the build needs no system LZ4.
use std::fs;
use std::path::{Path, PathBuf};

fn collect_cc(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_cc(&path, out)?;
            continue;
        }
        // Engine sources use the.cpp extension (renamed from.cc in the STL
        // rename task); accept both so the crate builds regardless.
        match path.extension().and_then(|s| s.to_str()) {
            Some("cpp") | Some("cc") => out.push(path),
            _ => {}
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // crate dir = crowdb-tree/ffi ; engine root = crowdb-tree
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let engine = manifest
        .parent()
        .ok_or("CARGO_MANIFEST_DIR must have a parent crowdb-tree dir")?
        .to_path_buf();
    let src = engine.join("src");
    let include = engine.join("include");
    // crowdb-common shared utils (R12): crc32c, log, compressing_sink, gzip,
    // metrics moved out of crowdb-tree into a sibling `crowdb-common/cpp` project.
    // The FFI build globs both source trees into one `cc::Build` so the moved
    // TUs compile with the same flags as the remaining crowdb-tree sources.
    let common = engine
        .parent()
        .expect("crowdb-tree must have a parent")
        .join("crowdb-common")
        .join("cpp");
    let common_src = common.join("src");
    let common_include = common.join("include");

    let mut files = Vec::new();
    collect_cc(&src, &mut files)?;
    collect_cc(&common_src, &mut files)?;

    let mut build = cc::Build::new();
    build
        .cpp(true)
        .std("c++20")
        // Line-tables-only debug info for C++: enough for GDB
        // backtraces (file:line) from core dumps, ~1MB per object.
        // .debug(false) guards against cc-rs inheriting a future
        // Cargo profile debug setting (which maps to full -g).
        .debug(false)
        .flag("-g1")
        .include(&include)
        .include(&common_include)
        .warnings(false);

    // The engine now includes Abseil headers (absl::btree_map in the MemTable,
    // ). Abseil is header-only for btree, so we only need its include
    // path; in the pixi/conda environment it lives under $CONDA_PREFIX/include.
    // Use -isystem (not -I) for third-party headers so -Wall -Wextra skips them.
    let conda_prefix = std::env::var("CONDA_PREFIX").ok().map(PathBuf::from).or_else(|| {
        // Fallback: when CONDA_PREFIX is not set (e.g., Playwright's
        // webServer subprocess), try the pixi env dir relative to CARGO_MANIFEST_DIR.
        let pixi_env = manifest
            .parent()
            .and_then(|p| p.parent())
            .map(|root| root.join(".pixi").join("envs").join("default"));
        pixi_env.filter(|p| p.join("include").is_dir())
    });
    if let Some(prefix) = &conda_prefix {
        let inc = prefix.join("include");
        if inc.join("absl").is_dir() {
            build.flag(format!("-isystem{}", inc.display()));
        }
    }
    // Fallback: when CONDA_PREFIX is not set (e.g., the pre-commit hook runs in a
    // bare shell), try common system install locations for Abseil headers.
    #[cfg(target_os = "macos")]
    {
        let homebrew = PathBuf::from("/opt/homebrew/include");
        if homebrew.join("absl").is_dir() {
            build.flag(format!("-isystem{}", homebrew.display()));
        }
    }
    {
        let local = PathBuf::from("/usr/local/include");
        if local.join("absl").is_dir() {
            build.flag(format!("-isystem{}", local.display()));
        }
    }
    println!("cargo:rerun-if-env-changed=CONDA_PREFIX");

    // liburing (io_uring reactor) -- Linux-only, no
    // macOS build exists, so reactor.cpp (now in crowdb-common) and
    // block_async_page_store.cpp (still in crowdb-tree) are excluded from
    // the compiled set entirely when not found, mirroring
    // crowdb-common/cpp/CMakeLists.txt's CROWDB_HAVE_LIBURING gate exactly
    // (same reasoning: macOS dev-path note).
    let liburing_dir = conda_prefix.as_ref().filter(|prefix| {
        prefix.join("include").join("liburing.h").is_file()
            && (prefix.join("lib").join("liburing.so").is_file()
                || prefix.join("lib").join("liburing.a").is_file())
    });
    if liburing_dir.is_none() {
        files.retain(|f| {
            !matches!(
                f.file_name().and_then(|s| s.to_str()),
                Some("reactor.cpp") | Some("block_async_page_store.cpp") | Some("diskio_uring.cpp")
            )
        });
    }

    for f in &files {
        build.file(f);
        println!("cargo:rerun-if-changed={}", f.display());
    }

    if let Some(prefix) = liburing_dir {
        let inc = prefix.join("include");
        build.flag(format!("-isystem{}", inc.display()));
        build.define("CROWDB_HAVE_LIBURING", "1");
        println!("cargo:rustc-link-search=native={}", prefix.join("lib").display());
        println!("cargo:rustc-link-lib=dylib=uring");
    }

    let have_lz4 = std::env::var("CROWDB_TREE_LZ4_LIB").ok();
    if have_lz4.is_some() {
        build.define("CROWDB_TREE_HAVE_LZ4", "1");
    }

    // ── spdlog logging (mirrors CMakeLists.txt) ──
    // The FFI build now enables spdlog so the C++ engine writes
    // `crowdb-tree.log` alongside the Rust server's `log/` directory.
    // Requires spdlog + fmt + zlib in the conda/pixi environment.
    let have_spdlog = conda_prefix.as_ref().is_some_and(|prefix| {
        prefix.join("include").join("spdlog").is_dir()
            && (prefix.join("lib").join("libspdlog.dylib").is_file()
                || prefix.join("lib").join("libspdlog.so").is_file()
                || prefix.join("lib").join("libspdlog.a").is_file())
    });
    if have_spdlog {
        let prefix = conda_prefix.as_ref().unwrap();
        // CROWDB_HAVE_SPDLOG gates the moved crowdb-common log.h/compressing_sink.h
        // (the remaining crowdb-tree sources include crowdb-common/log.h and use
        // CR_LOG_*). The FFI build compiles everything in one cc::Build, so a
        // single define covers both trees; the moved files no longer reference
        // CROWDB_TREE_HAVE_SPDLOG.
        build.define("CROWDB_HAVE_SPDLOG", "1");
        // fmt is bundled with spdlog in conda-forge; its headers live under
        // include/fmt and the lib is libfmt. zlib is needed by
        // compressing_sink for gzip-rotated log files.
        let lib_dir = prefix.join("lib");
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
        println!("cargo:rustc-link-lib=dylib=spdlog");
        println!("cargo:rustc-link-lib=dylib=fmt");
        println!("cargo:rustc-link-lib=dylib=z");
        // Embed the rpath so the dynamic linker finds libspdlog at runtime
        // (pixi/conda lib dir is not in the default dyld search path).
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
    }

    // ── ISA-L for SIMD-optimized CRC32C (crc32_iscsi) ──
    // crowdb-common/crc32c.h calls crc32_iscsi which lives in libisal.
    // The CMake build links it via find_library(ISAL_LIB NAMES isal); the
    // FFI build must link it too since it compiles crowdb-common sources.
    // The include path must also be added so isa-l/crc.h is found even
    // when the Abseil check above does not add the conda include dir.
    let have_isal = conda_prefix.as_ref().is_some_and(|prefix| {
        prefix.join("include").join("isa-l").join("crc.h").is_file()
            && (prefix.join("lib").join("libisal.dylib").is_file()
                || prefix.join("lib").join("libisal.so").is_file()
                || prefix.join("lib").join("libisal.a").is_file())
    });
    if have_isal {
        let prefix = conda_prefix.as_ref().unwrap();
        let inc_dir = prefix.join("include");
        let lib_dir = prefix.join("lib");
        build.flag(format!("-isystem{}", inc_dir.display()));
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
        println!("cargo:rustc-link-lib=dylib=isal");
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
    }

    build.compile("crowdb-tree");

    if let Some(dir) = have_lz4 {
        println!("cargo:rustc-link-search=native={dir}");
        println!("cargo:rustc-link-lib=dylib=lz4");
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
    }
    println!("cargo:rerun-if-changed={}", include.display());
    println!("cargo:rerun-if-env-changed=CROWDB_TREE_LZ4_LIB");
    println!("cargo:rerun-if-env-changed=CONDA_PREFIX");
    Ok(())
}
