// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

// Compile the crow-rpc C++ engine (including the C ABI) into a static lib
// and link it into this crate. Mirrors crow-tree-ffi/build.rs.
use std::fs;
use std::path::{Path, PathBuf};

fn collect_cpp(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_cpp(&path, out)?;
            continue;
        }
        if path.extension().and_then(|s| s.to_str()) == Some("cpp") {
            out.push(path);
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let engine = manifest
        .parent()
        .ok_or("CARGO_MANIFEST_DIR must have a parent crow-rpc dir")?
        .to_path_buf();
    let src = engine.join("src");
    let include = engine.join("include");
    // crow-common headers (MpscQueue) — shared with crow-tree.
    let common_include = engine
        .parent()
        .ok_or("engine dir must have a parent lib dir")?
        .join("crow-common")
        .join("cpp")
        .join("include");

    let mut sources = Vec::new();
    collect_cpp(&src, &mut sources)?;

    // crow-common metrics + gzip sources. MetricsRegistry::global()
    // and the C ABI flush function live in metrics/. metrics.cpp's
    // check_rotate() calls gzip_compress_file(), so gzip.cpp is also
    // needed (even though the file-based flush path is not used in
    // production — the linker still needs the symbol).
    let common_src = engine
        .parent()
        .ok_or("engine dir must have a parent lib dir")?
        .join("crow-common")
        .join("cpp")
        .join("src");
    collect_cpp(&common_src.join("metrics"), &mut sources)?;
    sources.push(common_src.join("gzip.cpp"));

    // Exclude platform-specific engines that don't compile on this OS.
    let target = std::env::var("TARGET").unwrap_or_default();
    let is_linux = target.contains("linux");
    let is_macos = target.contains("darwin");

    sources.retain(|p| {
        let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if name == "epoll_engine.cpp" && !is_linux {
            return false;
        }
        if name == "kqueue_engine.cpp" && !is_macos {
            return false;
        }
        // RDMA stubs — excluded unless CROW_RPC_HAVE_RDMA is set.
        if (name == "rdma_transport.cpp" || name == "rdma_buffer_pool.cpp")
            && std::env::var("CROW_RPC_HAVE_RDMA").is_err()
        {
            return false;
        }
        true
    });

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
        .flag_if_supported("-Wall")
        .flag_if_supported("-Wextra")
        .flag_if_supported("-Werror")
        .flag_if_supported("-Wno-sign-compare")
        .include(&include)
        .include(&common_include)
        .files(sources.iter().map(|p| p.as_path()).collect::<Vec<_>>());

    // Generated flatbuffer headers. CMake generates these into
    // build/generated/ during the C++ test build, but cargo clippy / cargo
    // build may run before CMake. Generate them ourselves into OUT_DIR
    // so the build is self-contained (doesn't depend on CMake having run).
    let protocol_fbs_dir = engine
        .parent()
        .ok_or("engine dir must have a parent lib dir")?
        .join("crow-protocol")
        .join("src")
        .join("fbs");
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let gen_dir = out_dir.join("generated");
    fs::create_dir_all(&gen_dir)?;

    let fbs_files = [
        "ret_code.fbs",
        "msg_type.fbs",
        "common_type.fbs",
        "common_msg.fbs",
    ];
    let flatc = std::process::Command::new("flatc")
        .arg("--cpp")
        .arg("-o")
        .arg(&gen_dir)
        .args(fbs_files.iter().map(|f| protocol_fbs_dir.join(f)))
        .output();
    match flatc {
        Ok(out) if out.status.success() => {
            build.include(&gen_dir);
        }
        Ok(out) => {
            // flatc failed — fall back to CMake-generated dir if it exists
            // (local dev where CMake already ran).
            let cmake_gen = engine.join("build").join("generated");
            if cmake_gen.exists() {
                build.include(&cmake_gen);
            } else {
                panic!(
                    "flatc failed and no CMake-generated headers found. \
                     flatc stderr: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
            }
        }
        Err(_) => {
            // flatc not found — fall back to CMake-generated dir.
            let cmake_gen = engine.join("build").join("generated");
            if cmake_gen.exists() {
                build.include(&cmake_gen);
            } else {
                panic!("flatc not found and no CMake-generated headers found");
            }
        }
    }

    // Find flatbuffers headers via pixi env (CONDA_PREFIX or pixi's env).
    if let Ok(prefix) = std::env::var("CONDA_PREFIX") {
        build.include(format!("{prefix}/include"));
    }

    // Find folly headers (for ConcurrentHashMap — required, no fallback).
    // The pixi env has folly installed. folly::ConcurrentHashMap is
    // header-only (template), but its DCHECK/CHECK macros reference
    // glog types, so glog headers must be on the include path even
    // though we don't link glog (DCHECK is a no-op in release).
    // GLOG_USE_GLOG_EXPORT is required by glog 0.7+ to enable the
    // export header that defines GLOG_EXPORT/GLOG_NO_EXPORT.
    if let Ok(prefix) = std::env::var("CONDA_PREFIX") {
        let cmake_dir = format!("{prefix}/lib/cmake/folly");
        if Path::new(&cmake_dir).exists() {
            build.define("GLOG_USE_GLOG_EXPORT", "1");
        } else {
            panic!("folly not found in CONDA_PREFIX (required for crow-rpc)");
        }
    }

    build.compile("crow-rpc");

    // Link system libs.
    if is_linux {
        println!("cargo:rustc-link-lib=ibverbs");
        println!("cargo:rustc-link-lib=rdmacm");
    }

    // Link folly + glog. ConcurrentHashMap uses hazard pointers,
    // SharedMutex, and glog (CHECK/DCHECK) — all require the shared
    // libs at link time.
    if let Ok(prefix) = std::env::var("CONDA_PREFIX") {
        let cmake_dir = format!("{prefix}/lib/cmake/folly");
        if Path::new(&cmake_dir).exists() {
            println!("cargo:rustc-link-search={prefix}/lib");
            println!("cargo:rustc-link-lib=folly");
            println!("cargo:rustc-link-lib=glog");
        }
    }

    // zlib for gzip_compress_file (used by metrics check_rotate).
    println!("cargo:rustc-link-lib=z");

    // Rerun if any source or header changes.
    for src in &sources {
        println!("cargo:rerun-if-changed={}", src.display());
    }
    println!("cargo:rerun-if-changed={}", include.display());
    println!("cargo:rerun-if-changed={}", common_include.display());
    for f in &fbs_files {
        println!("cargo:rerun-if-changed={}", protocol_fbs_dir.join(f).display());
    }

    Ok(())
}
