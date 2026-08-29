// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Compile the crowdb-rpc C++ engine (including the C ABI) into a static lib
// and link it into this crate. Mirrors crowdb-tree-ffi/build.rs.
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

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
        .ok_or("CARGO_MANIFEST_DIR must have a parent crowdb-rpc dir")?
        .to_path_buf();
    let src = engine.join("src");
    let include = engine.join("include");
    // crowdb-common headers (MpscQueue) — shared with crowdb-tree.
    let common_include = engine
        .parent()
        .ok_or("engine dir must have a parent lib dir")?
        .join("crowdb-common")
        .join("cpp")
        .join("include");

    let mut sources = Vec::new();
    collect_cpp(&src, &mut sources)?;

    // crowdb-common metrics + gzip sources. MetricsRegistry::global()
    // and the C ABI flush function live in metrics/. metrics.cpp's
    // check_rotate() calls gzip_compress_file(), so gzip.cpp is also
    // needed (even though the file-based flush path is not used in
    // production — the linker still needs the symbol).
    let common_src = engine
        .parent()
        .ok_or("engine dir must have a parent lib dir")?
        .join("crowdb-common")
        .join("cpp")
        .join("src");
    collect_cpp(&common_src.join("metrics"), &mut sources)?;
    sources.push(common_src.join("gzip.cpp"));

    // ── spdlog logging (mirrors crowdb-tree-ffi/build.rs) ──
    // When spdlog is available in the conda/pixi env, enable
    // CROWDB_HAVE_SPDLOG and compile log.cpp + compressing_sink.cpp so
    // the CR_LOG_* macros route to an async file logger (separate
    // `crowdb-rpc-*.log` files alongside the Rust server's `log/` dir).
    // Without spdlog, the macros are zero-cost no-ops.
    let conda_prefix = std::env::var("CONDA_PREFIX").ok().map(PathBuf::from);
    let have_spdlog = conda_prefix.as_ref().is_some_and(|prefix| {
        prefix.join("include").join("spdlog").is_dir()
            && (prefix.join("lib").join("libspdlog.dylib").is_file()
                || prefix.join("lib").join("libspdlog.so").is_file()
                || prefix.join("lib").join("libspdlog.a").is_file())
    });
    if have_spdlog {
        sources.push(common_src.join("log.cpp"));
        sources.push(common_src.join("compressing_sink.cpp"));
    }

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
        // RDMA stubs — excluded unless CROWDB_RPC_HAVE_RDMA is set.
        if (name == "rdma_transport.cpp" || name == "rdma_buffer_pool.cpp")
            && std::env::var("CROWDB_RPC_HAVE_RDMA").is_err()
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

    // Enable spdlog in the C++ build (gates CR_LOG_* macros).
    if have_spdlog {
        build.define("CROWDB_HAVE_SPDLOG", "1");
    }

    // Generated flatbuffer C++ headers. The .fbs schemas live in
    // crowdb-protocol (single home for all proto types); run flatc --cpp
    // ourselves into OUT_DIR so this crate is self-contained and does not
    // depend on a prior `cmake -S lib/crowdb-rpc` having populated
    // lib/crowdb-rpc/build/generated. CI runs `cargo clippy --all-targets`
    // before any CMake build, so relying on the CMake build dir would fail
    // there with "common_msg_generated.h: No such file or directory".
    // The same .fbs set is used by lib/crowdb-rpc/CMakeLists.txt for the
    // standalone C++ tests; both paths emit identical headers.
    let protocol_fbs_dir = engine
        .parent()
        .ok_or("engine dir must have a parent lib dir")?
        .join("crowdb-protocol")
        .join("src")
        .join("fbs");
    let fbs_files = [
        "ret_code.fbs",
        "msg_type.fbs",
        "common_type.fbs",
        "common_msg.fbs",
        "diskio.fbs",
    ];
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let gen_dir = out_dir.join("crowdb-rpc-generated");
    fs::create_dir_all(&gen_dir)?;
    let flatc = find_flatc();
    let status = Command::new(&flatc)
        .arg("--cpp")
        .arg("-o")
        .arg(&gen_dir)
        .args(fbs_files.iter().map(|f| protocol_fbs_dir.join(f)))
        .status()
        .map_err(|e| format!("failed to run flatc at {}: {e}", flatc.display()))?;
    if !status.success() {
        return Err(format!("flatc --cpp failed for crowdb-rpc proto schemas (status {status})").into());
    }
    build.include(&gen_dir);
    for f in &fbs_files {
        println!("cargo:rerun-if-changed={}", protocol_fbs_dir.join(f).display());
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
            panic!("folly not found in CONDA_PREFIX (required for crowdb-rpc)");
        }
    }

    build.compile("crowdb-rpc");

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

    // zlib for gzip_compress_file (used by metrics check_rotate and
    // compressing_sink's gzip-rotated log files).
    println!("cargo:rustc-link-lib=z");

    // Link spdlog + fmt for the C++ async file logger (CR_LOG_* macros).
    // fmt is bundled with spdlog in conda-forge. The rpath embedding
    // ensures the dynamic linker finds libspdlog at runtime (pixi/conda
    // lib dir is not in the default dyld search path).
    if have_spdlog {
        if let Some(prefix) = &conda_prefix {
            let lib_dir = prefix.join("lib");
            println!("cargo:rustc-link-search=native={}", lib_dir.display());
            println!("cargo:rustc-link-lib=dylib=spdlog");
            println!("cargo:rustc-link-lib=dylib=fmt");
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
        }
    }

    // Rerun if any source or header changes.
    for src in &sources {
        println!("cargo:rerun-if-changed={}", src.display());
    }
    println!("cargo:rerun-if-changed={}", include.display());
    println!("cargo:rerun-if-changed={}", common_include.display());

    Ok(())
}

/// Locate the `flatc` schema compiler. Pixi puts it in `$CONDA_PREFIX/bin`;
/// fall back to a pixi env dir relative to the manifest, then to `PATH`.
/// Mirrors crowdb-protocol/build.rs::find_flatc.
fn find_flatc() -> PathBuf {
    if let Ok(prefix) = std::env::var("CONDA_PREFIX") {
        let p = PathBuf::from(prefix).join("bin").join("flatc");
        if p.is_file() {
            return p;
        }
    }
    if let Ok(manifest) = std::env::var("CARGO_MANIFEST_DIR") {
        let pixi_env = PathBuf::from(manifest)
            .parent()
            .and_then(|p| p.parent())
            .map(|root| {
                root.join(".pixi")
                    .join("envs")
                    .join("default")
                    .join("bin")
                    .join("flatc")
            });
        if let Some(p) = pixi_env.filter(|p| p.is_file()) {
            return p;
        }
    }
    // Last resort: assume it is on PATH.
    PathBuf::from("flatc")
}
