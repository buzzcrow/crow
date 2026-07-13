// Compile the crowtree C++ engine (including the C ABI) into a static lib and
// link it into this crate. LZ4 is optional: set CROWTREE_LZ4_LIB=/path/to/dir
// containing liblz4 to enable on-disk compression; otherwise the codec degrades
// to identity (stored raw) and the build needs no system LZ4.
use std::fs;
use std::path::{Path, PathBuf};

fn collect_cc(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        // Engine sources use the .cpp extension (renamed from .cc in the STL
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

    let mut files = Vec::new();
    collect_cc(&src, &mut files)?;

    let mut build = cc::Build::new();
    build.cpp(true).std("c++20").include(&include).warnings(false);

    // The engine now includes Abseil headers (absl::btree_map in the MemTable,
    // plan-tree #9). Abseil is header-only for btree, so we only need its include
    // path; in the pixi/conda environment it lives under $CONDA_PREFIX/include.
    let conda_prefix = std::env::var("CONDA_PREFIX").ok().map(PathBuf::from);
    if let Some(prefix) = &conda_prefix {
        let inc = prefix.join("include");
        if inc.join("absl").is_dir() {
            build.include(&inc);
        }
    }
    // Fallback: when CONDA_PREFIX is not set (e.g., the pre-commit hook runs in a
    // bare shell), try common system install locations for Abseil headers.
    #[cfg(target_os = "macos")]
    {
        let homebrew = PathBuf::from("/opt/homebrew/include");
        if homebrew.join("absl").is_dir() {
            build.include(&homebrew);
        }
    }
    {
        let local = PathBuf::from("/usr/local/include");
        if local.join("absl").is_dir() {
            build.include(&local);
        }
    }
    println!("cargo:rerun-if-env-changed=CONDA_PREFIX");

    // liburing (io_uring reactor, plan-tree #11 Phase 0/1) -- Linux-only, no
    // macOS build exists, so reactor.cpp/file_async_page_store.cpp are
    // excluded from the compiled set entirely when not found, mirroring
    // crowtree/CMakeLists.txt's CROWTREE_HAVE_LIBURING gate exactly (same
    // reasoning: design-crowtree-async.md §10's macOS dev-path note).
    let liburing_dir = conda_prefix.as_ref().filter(|prefix| {
        prefix.join("include").join("liburing.h").is_file()
            && (prefix.join("lib").join("liburing.so").is_file()
                || prefix.join("lib").join("liburing.a").is_file())
    });
    if liburing_dir.is_none() {
        files.retain(|f| {
            !matches!(
                f.file_name().and_then(|s| s.to_str()),
                Some("reactor.cpp") | Some("file_async_page_store.cpp")
            )
        });
    }

    for f in &files {
        build.file(f);
        println!("cargo:rerun-if-changed={}", f.display());
    }

    if let Some(prefix) = liburing_dir {
        build.include(prefix.join("include"));
        build.define("CROWTREE_HAVE_LIBURING", "1");
        println!("cargo:rustc-link-search=native={}", prefix.join("lib").display());
        println!("cargo:rustc-link-lib=dylib=uring");
    }

    let have_lz4 = std::env::var("CROWTREE_LZ4_LIB").ok();
    if have_lz4.is_some() {
        build.define("CROWTREE_HAVE_LZ4", "1");
    }
    build.compile("crowtree");

    if let Some(dir) = have_lz4 {
        println!("cargo:rustc-link-search=native={dir}");
        println!("cargo:rustc-link-lib=dylib=lz4");
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}");
    }
    println!("cargo:rerun-if-changed={}", include.display());
    println!("cargo:rerun-if-env-changed=CROWTREE_LZ4_LIB");
    Ok(())
}
