// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Compile the crowtree C++ engine (including the C ABI) into a static lib and
// link it into this crate. LZ4 is optional: set CROWTREE_LZ4_LIB=/path/to/dir
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
    // crate dir = crowtree/ffi ; engine root = crowtree
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let engine = manifest
        .parent()
        .ok_or("CARGO_MANIFEST_DIR must have a parent crowtree dir")?
        .to_path_buf();
    let src = engine.join("src");
    let include = engine.join("include");
    // crow-common shared utils (R12): crc32c, log, compressing_sink, gzip,
    // metrics moved out of crowtree into a sibling `crow-common/cpp` project.
    // The FFI build globs both source trees into one `cc::Build` so the moved
    // TUs compile with the same flags as the remaining crowtree sources.
    let common = engine
        .parent()
        .expect("crowtree must have a parent")
        .join("crow-common")
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
        .include(&include)
        .include(&common_include)
        .warnings(false);

    // The engine now includes Abseil headers (absl::btree_map in the MemTable,
    // ). Abseil is header-only for btree, so we only need its include
    // path; in the pixi/conda environment it lives under $CONDA_PREFIX/include.
    // Use -isystem (not -I) for third-party headers so -Wall -Wextra skips them.
    let conda_prefix = std::env::var("CONDA_PREFIX").ok().map(PathBuf::from);
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
    // macOS build exists, so reactor.cpp/block_async_page_store.cpp are
    // excluded from the compiled set entirely when not found, mirroring
    // crowtree/CMakeLists.txt's CROWTREE_HAVE_LIBURING gate exactly (same
    // reasoning: macOS dev-path note).
    let liburing_dir = conda_prefix.as_ref().filter(|prefix| {
        prefix.join("include").join("liburing.h").is_file()
            && (prefix.join("lib").join("liburing.so").is_file()
                || prefix.join("lib").join("liburing.a").is_file())
    });
    if liburing_dir.is_none() {
        files.retain(|f| {
            !matches!(
                f.file_name().and_then(|s| s.to_str()),
                Some("reactor.cpp") | Some("block_async_page_store.cpp")
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
        build.define("CROWTREE_HAVE_LIBURING", "1");
        println!("cargo:rustc-link-search=native={}", prefix.join("lib").display());
        println!("cargo:rustc-link-lib=dylib=uring");
    }

    let have_lz4 = std::env::var("CROWTREE_LZ4_LIB").ok();
    if have_lz4.is_some() {
        build.define("CROWTREE_HAVE_LZ4", "1");
    }

    // ── spdlog logging (mirrors CMakeLists.txt) ──
    // The FFI build now enables spdlog so the C++ engine writes
    // `crowtree.log` alongside the Rust server's `log/` directory.
    // Requires spdlog + fmt + zlib in the conda/pixi environment.
    let have_spdlog = conda_prefix.as_ref().is_some_and(|prefix| {
        prefix.join("include").join("spdlog").is_dir()
            && (prefix.join("lib").join("libspdlog.dylib").is_file()
                || prefix.join("lib").join("libspdlog.so").is_file()
                || prefix.join("lib").join("libspdlog.a").is_file())
    });
    if have_spdlog {
        let prefix = conda_prefix.as_ref().unwrap();
        // CROW_HAVE_SPDLOG gates the moved crow-common log.h/compressing_sink.h
        // (the remaining crowtree sources include crow-common/log.h and use
        // CR_LOG_*). The FFI build compiles everything in one cc::Build, so a
        // single define covers both trees; the moved files no longer reference
        // CROWTREE_HAVE_SPDLOG.
        build.define("CROW_HAVE_SPDLOG", "1");
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

    build.compile("crowtree");

    if let Some(dir) = have_lz4 {
        println!("cargo:rustc-link-search=native={dir}");
        println!("cargo:rustc-link-lib=dylib=lz4");
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
    }
    println!("cargo:rerun-if-changed={}", include.display());
    println!("cargo:rerun-if-env-changed=CROWTREE_LZ4_LIB");
    println!("cargo:rerun-if-env-changed=CONDA_PREFIX");
    Ok(())
}
